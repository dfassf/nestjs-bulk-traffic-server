import { Request } from 'express';
import { WorkloadType } from '../interfaces/queue-task.interface';

export interface RequestRoutingDecision {
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

export class QueueRequestAnalyzer {
  constructor(private readonly requestProcessingTimeoutMs: number) {}

  analyze(req: Request): RequestRoutingDecision {
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
