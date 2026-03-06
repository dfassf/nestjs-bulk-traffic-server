import { Injectable } from '@nestjs/common';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';
import { readPositiveIntEnv } from './utils/env';

type DispatchableWorkloadType =
  | WorkloadType.CPU
  | WorkloadType.MEMORY
  | WorkloadType.CUSTOM;

@Injectable()
export class WorkerTaskRouterService {
  private readonly workerCount = readPositiveIntEnv('WORKER_POOL_SIZE', 4);
  private readonly cpuConcurrencyLimit = this.clampConcurrencyLimit(
    readPositiveIntEnv('WORKER_MAX_CPU_CONCURRENCY', this.workerCount),
  );
  private readonly memoryConcurrencyLimit = this.clampConcurrencyLimit(
    readPositiveIntEnv(
      'WORKER_MAX_MEMORY_CONCURRENCY',
      Math.max(1, Math.floor(this.workerCount / 2)),
    ),
  );
  private readonly customConcurrencyLimit = this.clampConcurrencyLimit(
    readPositiveIntEnv(
      'WORKER_MAX_CUSTOM_CONCURRENCY',
      Math.max(1, Math.floor(this.workerCount / 2)),
    ),
  );

  private activeByType: Record<DispatchableWorkloadType, number> = {
    [WorkloadType.CPU]: 0,
    [WorkloadType.MEMORY]: 0,
    [WorkloadType.CUSTOM]: 0,
  };
  private dispatchedByType: Record<DispatchableWorkloadType, number> = {
    [WorkloadType.CPU]: 0,
    [WorkloadType.MEMORY]: 0,
    [WorkloadType.CUSTOM]: 0,
  };
  droppedUnknownWorkloadCount = 0;

  get limits(): Record<DispatchableWorkloadType, number> {
    return {
      [WorkloadType.CPU]: this.cpuConcurrencyLimit,
      [WorkloadType.MEMORY]: this.memoryConcurrencyLimit,
      [WorkloadType.CUSTOM]: this.customConcurrencyLimit,
    };
  }

  getActiveByType(): Record<DispatchableWorkloadType, number> {
    return { ...this.activeByType };
  }

  getDispatchedByType(): Record<DispatchableWorkloadType, number> {
    return { ...this.dispatchedByType };
  }

  determineTaskType(taskData: WorkerTaskData): WorkloadType {
    if (taskData.type) return taskData.type;

    const task = taskData.task;
    if (task.workloadType) return task.workloadType;

    if (taskData.functionCode || task.functionCode) return WorkloadType.CUSTOM;
    return WorkloadType.UNKNOWN;
  }

  determineOperation(taskData: WorkerTaskData): string {
    if (taskData.operation) return taskData.operation;

    const type = this.determineTaskType(taskData);
    const category = taskData.task.category || '';

    if (type === WorkloadType.CPU) {
      if (category.includes('prime')) return 'findPrimes';
      if (category.includes('fibonacci')) return 'fibonacci';
      if (category.includes('matrix')) return 'matrixMultiply';
      return 'findPrimes';
    }

    if (type === WorkloadType.MEMORY) {
      if (category.includes('array')) return 'largeArray';
      if (category.includes('object') || category.includes('clone')) {
        return 'objectCloning';
      }
      return 'largeArray';
    }

    if (type === WorkloadType.CUSTOM) return 'execute';
    return 'findPrimes';
  }

  isDispatchableType(type: WorkloadType): type is DispatchableWorkloadType {
    return (
      type === WorkloadType.CPU ||
      type === WorkloadType.MEMORY ||
      type === WorkloadType.CUSTOM
    );
  }

  canDispatchType(type: DispatchableWorkloadType): boolean {
    return this.activeByType[type] < this.getConcurrencyLimit(type);
  }

  incrementActiveByType(type: DispatchableWorkloadType): void {
    this.activeByType[type] += 1;
    this.dispatchedByType[type] += 1;
  }

  decrementActiveByType(type: WorkloadType): void {
    if (!this.isDispatchableType(type)) return;
    this.activeByType[type] = Math.max(0, this.activeByType[type] - 1);
  }

  dequeueDispatchableTask(
    queue: WorkerTaskData[],
    onTaskFailed: (task: QueueTask) => void,
  ): { taskData: WorkerTaskData; type: DispatchableWorkloadType } | null {
    if (queue.length === 0) return null;

    for (let i = 0; i < queue.length; i++) {
      const candidate = queue[i];
      const type = this.determineTaskType(candidate);

      if (!this.isDispatchableType(type)) {
        queue.splice(i, 1);
        this.droppedUnknownWorkloadCount += 1;
        candidate.task.reject(
          new Error('알 수 없는 workloadType은 워커로 처리할 수 없습니다.'),
        );
        onTaskFailed(candidate.task);
        i--;
        continue;
      }

      if (!this.canDispatchType(type)) {
        continue;
      }

      queue.splice(i, 1);
      candidate.type = type;
      return { taskData: candidate, type };
    }

    return null;
  }

  getPendingQueueByType(queue: WorkerTaskData[]): Record<WorkloadType, number> {
    const queueByType: Record<WorkloadType, number> = {
      [WorkloadType.CPU]: 0,
      [WorkloadType.MEMORY]: 0,
      [WorkloadType.CUSTOM]: 0,
      [WorkloadType.UNKNOWN]: 0,
    };

    for (const taskData of queue) {
      const type = this.determineTaskType(taskData);
      queueByType[type] += 1;
    }

    return queueByType;
  }

  resetActiveByType(): void {
    this.activeByType = {
      [WorkloadType.CPU]: 0,
      [WorkloadType.MEMORY]: 0,
      [WorkloadType.CUSTOM]: 0,
    };
  }

  private clampConcurrencyLimit(limit: number): number {
    return Math.max(1, Math.min(limit, this.workerCount));
  }

  private getConcurrencyLimit(type: DispatchableWorkloadType): number {
    if (type === WorkloadType.CPU) return this.cpuConcurrencyLimit;
    if (type === WorkloadType.MEMORY) return this.memoryConcurrencyLimit;
    return this.customConcurrencyLimit;
  }
}
