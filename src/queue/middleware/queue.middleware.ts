import { Injectable, Logger, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { QueueService } from '../queue.service';
import { QueueRequestAnalyzer } from './queue-request-analyzer';
import { RequestStateStore } from './request-state.store';
import { readPositiveIntEnv } from '../utils/env';

@Injectable()
export class QueueMiddleware implements NestMiddleware {
  private readonly logger = new Logger(QueueMiddleware.name);
  // 미들웨어를 지나는 요청은 analyzer 가 항상 timeout 을 채워 넣기 때문에,
  // 이 값이 곧 HTTP 경로의 실행 타임아웃이 된다. 여기서 환경변수를 안 읽으면
  // QUEUE_EXECUTION_TIMEOUT_MS 를 조정해도 HTTP 요청에는 아무 효과가 없다.
  private readonly requestProcessingTimeoutMs = readPositiveIntEnv(
    'QUEUE_EXECUTION_TIMEOUT_MS',
    10000,
  );
  private readonly requestStateTtlMs = 30000;
  private readonly allowCustomWorkload =
    process.env.ALLOW_CUSTOM_WORKLOAD === 'true';
  private readonly analyzer = new QueueRequestAnalyzer(
    this.requestProcessingTimeoutMs,
  );
  private readonly requestState = new RequestStateStore(this.requestStateTtlMs);

  constructor(private readonly queueService: QueueService) {}

  async use(req: Request, res: Response, next: NextFunction): Promise<void> {
    const decision = this.analyzer.analyze(req);

    if (decision.bypass) {
      next();
      return;
    }

    const requestId = randomUUID();
    this.requestState.register(requestId);
    const requestStartedAt = Date.now();

    try {
      const stats = this.queueService.getQueueStats();

      if (stats.memoryPressure && decision.priority < 0) {
        this.requestState.markResponded(requestId);
        if (!res.headersSent) {
          res.status(503).send({
            error: '서비스 과부하',
            message: '서버가 과부하 상태입니다. 잠시 후 다시 시도해주세요.',
          });
        }
        return;
      }

      if (decision.functionCode && !this.allowCustomWorkload) {
        this.requestState.markResponded(requestId);
        if (!res.headersSent) {
          res.status(403).send({
            error: '기능 비활성화',
            message: 'custom workload 기능이 비활성화되어 있습니다.',
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
      const message =
        error instanceof Error ? error.message : '알 수 없는 오류';
      this.logger.error(`[${requestId}] 큐 처리 중 오류: ${message}`);

      const isCustomDisabled = message.includes(
        'custom workload 기능이 비활성화',
      );
      const isValidationError =
        message.includes('문자열이어야') ||
        message.includes('정수여야') ||
        message.includes('빈 문자열') ||
        message.includes('값은 0보다 큰 정수');
      const statusCode = isCustomDisabled ? 403 : isValidationError ? 400 : 503;
      const errorCode = isCustomDisabled
        ? '기능 비활성화'
        : isValidationError
          ? '잘못된 요청'
          : '서비스 일시적으로 사용 불가';

      if (!this.requestState.hasResponded(requestId) && !res.headersSent) {
        this.requestState.markResponded(requestId);
        res.status(statusCode).send({
          error: errorCode,
          message,
        });
      }
    } finally {
      this.requestState.release(requestId);
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
        this.requestState.markResponded(requestId);
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

        if (!res.headersSent && !this.requestState.hasResponded(requestId)) {
          this.requestState.markResponded(requestId);
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
}
