import { Injectable, Logger, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { QueueService } from '../queue.service';
import { WorkloadType } from '../interfaces/queue-task.interface';

interface RequestState {
  responded: boolean;
  cleanupTimer: ReturnType<typeof setTimeout>;
}

interface RequestRoutingDecision {
  priority: number;
  category: string;
  isBatchable: boolean;
  size: number;
  bypass: boolean;
  timeout: number;
  workloadType?: WorkloadType | string;
  params?: Record<string, unknown>;
  functionCode?: string;
}

@Injectable()
export class QueueMiddleware implements NestMiddleware {
  private readonly logger = new Logger(QueueMiddleware.name);
  private readonly requestStateMap = new Map<string, RequestState>();
  private readonly requestProcessingTimeoutMs = 10000;
  private readonly requestStateTtlMs = 30000;

  constructor(private readonly queueService: QueueService) {}

  async use(req: Request, res: Response, next: NextFunction): Promise<void> {
    const decision = this.analyzeRequest(req);

    if (decision.bypass) {
      next();
      return;
    }

    const requestId = randomUUID();
    this.registerRequest(requestId);
    const requestStartedAt = Date.now();

    try {
      const stats = this.queueService.getQueueStats();

      if (stats.memoryPressure && decision.priority < 0) {
        this.markResponded(requestId);
        if (!res.headersSent) {
          res.status(503).send({
            error: '서비스 과부하',
            message: '서버가 과부하 상태입니다. 잠시 후 다시 시도해주세요.',
          });
        }
        return;
      }

      await this.queueService.enqueue(
        () =>
          this.executeQueuedRequest({
            req,
            res,
            next,
            requestId,
            requestStartedAt,
            timeoutMs: decision.timeout,
          }),
        {
          priority: decision.priority,
          requestId,
          category: decision.category,
          batch: decision.isBatchable,
          size: decision.size,
          timeout: decision.timeout,
          workloadType: decision.workloadType,
          params: decision.params,
          functionCode: decision.functionCode,
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : '알 수 없는 오류';
      this.logger.error(`[${requestId}] 큐 처리 중 오류: ${message}`);

      if (!this.hasResponded(requestId) && !res.headersSent) {
        this.markResponded(requestId);
        res.status(503).send({
          error: '서비스 일시적으로 사용 불가',
          message,
        });
      }
    } finally {
      this.releaseRequest(requestId);
    }
  }

  private executeQueuedRequest(input: {
    req: Request;
    res: Response;
    next: NextFunction;
    requestId: string;
    requestStartedAt: number;
    timeoutMs: number;
  }): Promise<void> {
    const { req, res, next, requestId, requestStartedAt, timeoutMs } = input;

    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const queueWaitTime = Date.now() - requestStartedAt;

      const finish = (): void => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve();
      };

      const onResponseFinished = (): void => {
        this.markResponded(requestId);
        const totalDuration = Date.now() - requestStartedAt;

        if (totalDuration > 5000) {
          this.logger.warn(
            `[${requestId}] 응답 지연 - total=${totalDuration}ms, queue=${queueWaitTime}ms, execute=${Math.max(0, totalDuration - queueWaitTime)}ms (${req.method} ${req.path})`,
          );
        }

        finish();
      };

      const timeoutId = setTimeout(() => {
        if (settled) return;

        this.logger.warn(
          `[${requestId}] 요청 처리 타임아웃 ${timeoutMs}ms (${req.method} ${req.path})`,
        );

        if (!res.headersSent && !this.hasResponded(requestId)) {
          this.markResponded(requestId);
          res.status(408).send({
            error: '요청 처리 시간 초과',
            message: '요청 처리가 너무 오래 걸립니다.',
          });
        }

        finish();
      }, timeoutMs);

      const cleanup = (): void => {
        clearTimeout(timeoutId);
        res.off('finish', onResponseFinished);
        res.off('close', onResponseFinished);
      };

      res.once('finish', onResponseFinished);
      res.once('close', onResponseFinished);

      try {
        next();
      } catch (error) {
        cleanup();
        settled = true;
        reject(error);
      }
    });
  }

  private registerRequest(requestId: string): void {
    const cleanupTimer = setTimeout(() => {
      this.requestStateMap.delete(requestId);
    }, this.requestStateTtlMs);

    this.requestStateMap.set(requestId, {
      responded: false,
      cleanupTimer,
    });
  }

  private markResponded(requestId: string): void {
    const state = this.requestStateMap.get(requestId);
    if (!state) return;
    state.responded = true;
  }

  private hasResponded(requestId: string): boolean {
    return this.requestStateMap.get(requestId)?.responded ?? false;
  }

  private releaseRequest(requestId: string): void {
    const state = this.requestStateMap.get(requestId);
    if (state) {
      clearTimeout(state.cleanupTimer);
    }
    this.requestStateMap.delete(requestId);
  }

  private analyzeRequest(req: Request): RequestRoutingDecision {
    const path = (req.path || req.url || '').toLowerCase();
    const method = (req.method || 'GET').toUpperCase();

    let priority = 0;
    let category = 'default';
    let isBatchable = false;
    let size = 1;
    let bypass = false;
    let timeout = this.requestProcessingTimeoutMs;
    let workloadType: WorkloadType | string | undefined;

    if (this.shouldBypass(path)) {
      bypass = true;
    }

    size += this.calculateBodySizeWeight(req.body);

    if (method === 'GET') {
      priority += 2;
      if (!req.query || Object.keys(req.query).length === 0) {
        isBatchable = true;
      }
    } else if (method === 'DELETE') {
      priority -= 1;
    } else if (method === 'PUT' || method === 'PATCH') {
      priority -= 2;
    } else if (method === 'POST') {
      priority -= 3;
      size += 1;
    }

    if (path.includes('/admin')) {
      priority += 5;
      category = 'admin';
    } else if (path.includes('bulk') || path.includes('batch')) {
      priority -= 5;
      category = 'bulk';
      isBatchable = true;
      size += 3;
      timeout = 15000;
    } else if (path.includes('analytics') || path.includes('report')) {
      priority -= 3;
      category = 'analytics';
      isBatchable = true;
      size += 2;
      timeout = 15000;
    } else if (path.includes('user') || path.includes('account')) {
      category = 'user';
      if (method === 'GET') {
        priority += 1;
      }
    }

    const bodyRecord = this.asObject(req.body);

    if (bodyRecord?.workloadType && typeof bodyRecord.workloadType === 'string') {
      workloadType = bodyRecord.workloadType.trim().toLowerCase();
    }

    if (!workloadType) {
      if (
        path.includes('cpu') ||
        path.includes('prime') ||
        path.includes('fibonacci') ||
        path.includes('matrix')
      ) {
        workloadType = WorkloadType.CPU;
      } else if (
        path.includes('memory') ||
        path.includes('array') ||
        path.includes('clone')
      ) {
        workloadType = WorkloadType.MEMORY;
      }
    }

    const functionCode =
      typeof bodyRecord?.functionCode === 'string'
        ? bodyRecord.functionCode
        : undefined;

    if (functionCode) {
      workloadType = WorkloadType.CUSTOM;
    }

    return {
      priority,
      category,
      isBatchable,
      size: Math.max(1, size),
      bypass,
      timeout,
      workloadType,
      params: this.extractParams(bodyRecord),
      functionCode,
    };
  }

  private shouldBypass(path: string): boolean {
    if (
      path === '/health' ||
      path === '/queue-stats' ||
      path.includes('/status') ||
      path.includes('/ping')
    ) {
      return true;
    }

    return /\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|eot)$/.test(path);
  }

  private calculateBodySizeWeight(body: unknown): number {
    if (!body) return 0;

    try {
      const bodySize = Buffer.byteLength(JSON.stringify(body), 'utf8');
      if (bodySize > 10000) return 4;
      if (bodySize > 3000) return 2;
      if (bodySize > 1000) return 1;
      return 0;
    } catch {
      return 1;
    }
  }

  private asObject(value: unknown): Record<string, unknown> | undefined {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return undefined;
    }
    return value as Record<string, unknown>;
  }

  private extractParams(
    bodyRecord?: Record<string, unknown>,
  ): Record<string, unknown> | undefined {
    if (!bodyRecord) return undefined;

    const fromParams = this.asObject(bodyRecord.params);
    const source = fromParams || bodyRecord;

    const { workloadType, functionCode, params, ...rest } = source;
    if (Object.keys(rest).length === 0) {
      return undefined;
    }

    return rest;
  }
}
