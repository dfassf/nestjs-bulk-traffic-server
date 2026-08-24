import { Injectable, Logger } from '@nestjs/common';
import {
  EnqueueOptions,
  WorkloadType,
} from './interfaces/queue-task.interface';

export interface NormalizedEnqueueOptions {
  priority: number;
  category: string;
  size: number;
  timeout: number;
  workloadType?: WorkloadType;
  params: Record<string, unknown>;
  functionCode?: string;
  batch: boolean;
  requestId?: string | number;
}

@Injectable()
export class QueueOptionsParser {
  private readonly logger = new Logger(QueueOptionsParser.name);

  private readonly allowCustomWorkload =
    process.env.ALLOW_CUSTOM_WORKLOAD === 'true';

  workloadGeneralQueueFallbackCount = 0;

  parsePositiveIntegerOption(
    value: unknown,
    optionName: string,
    fallback: number,
  ): number {
    if (value === undefined || value === null) {
      return fallback;
    }

    const parsed =
      typeof value === 'number'
        ? value
        : typeof value === 'string'
          ? Number(value)
          : Number.NaN;
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error(`${optionName} 값은 0보다 큰 정수여야 합니다.`);
    }

    return parsed;
  }

  parsePriorityOption(value: unknown): number {
    if (value === undefined || value === null) {
      return 0;
    }

    const parsed =
      typeof value === 'number'
        ? value
        : typeof value === 'string'
          ? Number(value)
          : Number.NaN;
    if (!Number.isInteger(parsed)) {
      throw new Error('priority는 정수여야 합니다.');
    }

    return parsed;
  }

  parseCategoryOption(value: unknown): string {
    if (value === undefined || value === null) {
      return 'default';
    }

    if (typeof value !== 'string') {
      throw new Error('category는 문자열이어야 합니다.');
    }

    const normalized = value.trim();
    if (!normalized) {
      throw new Error('category는 빈 문자열일 수 없습니다.');
    }

    return normalized;
  }

  parseParamsOption(value: unknown): Record<string, unknown> {
    if (value === undefined || value === null) {
      return {};
    }

    if (typeof value !== 'object' || Array.isArray(value)) {
      throw new Error('params는 객체 형태여야 합니다.');
    }

    return value as Record<string, unknown>;
  }

  parseFunctionCodeOption(value: unknown): string | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    if (typeof value !== 'string') {
      throw new Error('functionCode는 문자열이어야 합니다.');
    }

    if (!this.allowCustomWorkload) {
      throw new Error('custom workload 기능이 비활성화되어 있습니다.');
    }

    return value;
  }

  parseWorkloadTypeOption(value: unknown): WorkloadType | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    if (typeof value !== 'string') {
      throw new Error('workloadType은 문자열이어야 합니다.');
    }

    const normalized = value.trim().toLowerCase();
    if (normalized === WorkloadType.CPU) return WorkloadType.CPU;
    if (normalized === WorkloadType.MEMORY) return WorkloadType.MEMORY;
    if (normalized === WorkloadType.CUSTOM) {
      if (!this.allowCustomWorkload) {
        throw new Error('custom workload 기능이 비활성화되어 있습니다.');
      }
      return WorkloadType.CUSTOM;
    }
    if (normalized === WorkloadType.UNKNOWN) {
      this.workloadGeneralQueueFallbackCount++;
      return WorkloadType.UNKNOWN;
    }

    this.workloadGeneralQueueFallbackCount++;
    this.logger.warn(
      `알 수 없는 workloadType(${value}) 입력으로 일반 큐 처리로 fallback 합니다.`,
    );
    return WorkloadType.UNKNOWN;
  }

  normalizeEnqueueOptions(
    options: EnqueueOptions,
    defaultTimeout: number,
  ): NormalizedEnqueueOptions {
    return {
      priority: this.parsePriorityOption(options.priority),
      category: this.parseCategoryOption(options.category),
      size: this.parsePositiveIntegerOption(options.size, 'size', 1),
      timeout: this.parsePositiveIntegerOption(
        options.timeout,
        'timeout',
        defaultTimeout,
      ),
      workloadType: this.parseWorkloadTypeOption(options.workloadType),
      params: this.parseParamsOption(options.params),
      functionCode: this.parseFunctionCodeOption(options.functionCode),
      batch: options.batch === true,
      requestId: options.requestId,
    };
  }
}
