import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Worker } from 'worker_threads';
import * as path from 'path';
import { existsSync } from 'fs';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';

interface WorkerMessage {
  success?: boolean;
  result?: unknown;
  error?: string;
  initialized?: boolean;
  healthCheck?: boolean;
  operation?: string;
  duration?: number;
}

type DispatchableWorkloadType =
  | WorkloadType.CPU
  | WorkloadType.MEMORY
  | WorkloadType.CUSTOM;

@Injectable()
export class WorkerPoolService implements OnModuleDestroy {
  private readonly logger = new Logger(WorkerPoolService.name);

  private readonly useWorkers =
    process.env.DISABLE_WORKERS !== 'true' && process.env.NODE_ENV !== 'test';
  private readonly workerCount = this.readPositiveIntEnv('WORKER_POOL_SIZE', 4);
  private readonly healthCheckIntervalMs = 10000;
  private readonly healthCheckTimeoutMs = 2000;
  private readonly cpuConcurrencyLimit = this.clampConcurrencyLimit(
    this.readPositiveIntEnv('WORKER_MAX_CPU_CONCURRENCY', this.workerCount),
  );
  private readonly memoryConcurrencyLimit = this.clampConcurrencyLimit(
    this.readPositiveIntEnv(
      'WORKER_MAX_MEMORY_CONCURRENCY',
      Math.max(1, Math.floor(this.workerCount / 2)),
    ),
  );
  private readonly customConcurrencyLimit = this.clampConcurrencyLimit(
    this.readPositiveIntEnv(
      'WORKER_MAX_CUSTOM_CONCURRENCY',
      Math.max(1, Math.floor(this.workerCount / 2)),
    ),
  );

  private workerPool: Array<Worker | null> = [];
  private workerBusy: boolean[] = [];
  private workerTaskQueue: WorkerTaskData[] = [];
  private assignedTasks = new Map<number, WorkerTaskData>();
  private pingTimeoutMap = new Map<number, ReturnType<typeof setTimeout>>();
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
  private droppedUnknownWorkloadCount = 0;

  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private onResult: ((workerId: number, result: WorkerMessage) => void) | null =
    null;
  private shuttingDown = false;

  get isEnabled(): boolean {
    return this.useWorkers;
  }

  get taskQueue(): WorkerTaskData[] {
    return this.workerTaskQueue;
  }

  getPoolStats(): {
    totalWorkers: number;
    busyWorkers: number;
    idleWorkers: number;
    pendingTasks: number;
    activeByType: Record<DispatchableWorkloadType, number>;
    queueByType: Record<WorkloadType, number>;
    limitsByType: Record<DispatchableWorkloadType, number>;
    dispatchedByType: Record<DispatchableWorkloadType, number>;
    droppedUnknownWorkloadCount: number;
    enabled: boolean;
  } {
    const totalWorkers = this.workerPool.filter(Boolean).length;
    const busyWorkers = this.workerBusy.filter(Boolean).length;
    const queueByType = this.getPendingQueueByType();

    return {
      totalWorkers,
      busyWorkers,
      idleWorkers: Math.max(0, totalWorkers - busyWorkers),
      pendingTasks: this.workerTaskQueue.length,
      activeByType: { ...this.activeByType },
      queueByType,
      limitsByType: {
        [WorkloadType.CPU]: this.cpuConcurrencyLimit,
        [WorkloadType.MEMORY]: this.memoryConcurrencyLimit,
        [WorkloadType.CUSTOM]: this.customConcurrencyLimit,
      },
      dispatchedByType: { ...this.dispatchedByType },
      droppedUnknownWorkloadCount: this.droppedUnknownWorkloadCount,
      enabled: this.useWorkers,
    };
  }

  initWorkerPool(onResult: (workerId: number, result: WorkerMessage) => void): void {
    if (!this.useWorkers) {
      this.logger.warn(
        '워커 시스템이 비활성화되었습니다. 메인 스레드 처리로 동작합니다.',
      );
      return;
    }

    this.onResult = onResult;

    for (let i = 0; i < this.workerCount; i++) {
      this.createOrReplaceWorker(i);
    }

    this.startHealthChecks();
    this.logger.log(`워커 풀 초기화 완료: ${this.workerCount}개`);
  }

  onModuleDestroy(): void {
    void this.destroy();
  }

  async destroy(): Promise<void> {
    this.shuttingDown = true;

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    for (const timer of this.pingTimeoutMap.values()) {
      clearTimeout(timer);
    }
    this.pingTimeoutMap.clear();

    const terminations: Array<Promise<number>> = [];
    for (const worker of this.workerPool) {
      if (worker) {
        terminations.push(worker.terminate());
      }
    }

    await Promise.allSettled(terminations);
    this.workerPool = [];
    this.workerBusy = [];
    this.workerTaskQueue = [];
    this.assignedTasks.clear();
    this.activeByType = {
      [WorkloadType.CPU]: 0,
      [WorkloadType.MEMORY]: 0,
      [WorkloadType.CUSTOM]: 0,
    };
    this.onResult = null;
  }

  addTask(taskData: WorkerTaskData): void {
    this.workerTaskQueue.push(taskData);
  }

  processWorkerTasks(onTaskFailed: (task: QueueTask) => void): void {
    if (!this.useWorkers || this.workerTaskQueue.length === 0) return;

    for (let i = 0; i < this.workerPool.length; i++) {
      const worker = this.workerPool[i];
      if (!worker) continue;
      if (this.workerBusy[i]) continue;
      if (this.workerTaskQueue.length === 0) break;

      const pickedTask = this.dequeueDispatchableTask(onTaskFailed);
      if (!pickedTask) continue;
      const { taskData, type } = pickedTask;

      try {
        worker.postMessage({
          type,
          operation: this.determineOperation(taskData),
          params: taskData.params || {},
          functionCode: taskData.functionCode ?? taskData.task.functionCode,
          timeout: taskData.timeout ?? taskData.task.timeout,
        });

        this.workerBusy[i] = true;
        this.incrementActiveByType(type);
        this.dispatchedByType[type] += 1;
        this.assignedTasks.set(i, taskData);
      } catch (error) {
        const message =
          error instanceof Error
            ? error.message
            : '워커 작업 전송 중 알 수 없는 오류';
        taskData.task.reject(new Error(`워커 작업 전송 실패: ${message}`));
        onTaskFailed(taskData.task);
        this.workerBusy[i] = false;
        this.assignedTasks.delete(i);
      }
    }
  }

  handleWorkerResult(
    workerId: number,
    result: WorkerMessage,
    onSuccess: (task: QueueTask) => void,
    onFailed: (task: QueueTask) => void,
  ): void {
    const taskData = this.assignedTasks.get(workerId);
    this.assignedTasks.delete(workerId);
    this.workerBusy[workerId] = false;

    if (!taskData) return;
    this.decrementActiveByType(this.determineTaskType(taskData));

    if (result.success) {
      taskData.task.resolve(result.result);
      onSuccess(taskData.task);
      return;
    }

    taskData.task.reject(new Error(result.error || '워커 처리 실패'));
    onFailed(taskData.task);
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

  async processWithoutWorkers(
    onSuccess: () => void,
    onError: (task: QueueTask) => void,
  ): Promise<void> {
    while (this.workerTaskQueue.length > 0) {
      const taskData = this.workerTaskQueue.shift();
      if (!taskData) continue;

      try {
        const result = await taskData.task.execute();
        taskData.task.resolve(result);
        onSuccess();
      } catch (error) {
        taskData.task.reject(
          error instanceof Error ? error : new Error('메인 스레드 처리 실패'),
        );
        onError(taskData.task);
      }
    }
  }

  private readPositiveIntEnv(name: string, fallback: number): number {
    const raw = process.env[name];
    if (!raw) return fallback;

    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  private clampConcurrencyLimit(limit: number): number {
    return Math.max(1, Math.min(limit, this.workerCount));
  }

  private isDispatchableType(
    type: WorkloadType,
  ): type is DispatchableWorkloadType {
    return (
      type === WorkloadType.CPU ||
      type === WorkloadType.MEMORY ||
      type === WorkloadType.CUSTOM
    );
  }

  private getConcurrencyLimit(type: DispatchableWorkloadType): number {
    if (type === WorkloadType.CPU) return this.cpuConcurrencyLimit;
    if (type === WorkloadType.MEMORY) return this.memoryConcurrencyLimit;
    return this.customConcurrencyLimit;
  }

  private canDispatchType(type: DispatchableWorkloadType): boolean {
    return this.activeByType[type] < this.getConcurrencyLimit(type);
  }

  private incrementActiveByType(type: DispatchableWorkloadType): void {
    this.activeByType[type] += 1;
  }

  private decrementActiveByType(type: WorkloadType): void {
    if (!this.isDispatchableType(type)) return;
    this.activeByType[type] = Math.max(0, this.activeByType[type] - 1);
  }

  private dequeueDispatchableTask(
    onTaskFailed: (task: QueueTask) => void,
  ): { taskData: WorkerTaskData; type: DispatchableWorkloadType } | null {
    if (this.workerTaskQueue.length === 0) return null;

    for (let i = 0; i < this.workerTaskQueue.length; i++) {
      const candidate = this.workerTaskQueue[i];
      const type = this.determineTaskType(candidate);

      if (!this.isDispatchableType(type)) {
        this.workerTaskQueue.splice(i, 1);
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

      this.workerTaskQueue.splice(i, 1);
      candidate.type = type;
      return { taskData: candidate, type };
    }

    return null;
  }

  private getPendingQueueByType(): Record<WorkloadType, number> {
    const queueByType: Record<WorkloadType, number> = {
      [WorkloadType.CPU]: 0,
      [WorkloadType.MEMORY]: 0,
      [WorkloadType.CUSTOM]: 0,
      [WorkloadType.UNKNOWN]: 0,
    };

    for (const taskData of this.workerTaskQueue) {
      const type = this.determineTaskType(taskData);
      queueByType[type] += 1;
    }

    return queueByType;
  }

  private resolveWorkerPath(): string {
    const distPath = path.resolve(__dirname, 'worker.js');
    if (existsSync(distPath)) return distPath;

    return path.resolve(process.cwd(), 'src/queue/worker.js');
  }

  private createOrReplaceWorker(index: number): void {
    const worker = new Worker(this.resolveWorkerPath());

    worker.on('message', (result: WorkerMessage) => {
      if (result.initialized) {
        this.workerBusy[index] = false;
        return;
      }

      if (result.healthCheck) {
        this.clearPingTimeout(index);
        return;
      }

      this.clearPingTimeout(index);
      this.onResult?.(index, result);
    });

    worker.on('error', (error) => {
      this.handleWorkerFailure(index, error);
    });

    worker.on('exit', (code) => {
      if (this.shuttingDown) return;
      if (code !== 0) {
        this.handleWorkerFailure(
          index,
          new Error(`워커 비정상 종료(code=${code})`),
        );
      }
    });

    this.workerPool[index] = worker;
    this.workerBusy[index] = false;
  }

  private startHealthChecks(): void {
    if (!this.useWorkers) return;

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    this.healthCheckTimer = setInterval(() => {
      for (let i = 0; i < this.workerPool.length; i++) {
        const worker = this.workerPool[i];
        if (!worker) continue;
        if (this.workerBusy[i]) continue;
        if (this.pingTimeoutMap.has(i)) continue;

        try {
          worker.postMessage({ type: 'system', operation: 'ping' });

          const timeout = setTimeout(() => {
            this.handleWorkerFailure(i, new Error('워커 헬스체크 타임아웃'));
          }, this.healthCheckTimeoutMs);

          this.pingTimeoutMap.set(i, timeout);
        } catch (error) {
          const wrapped =
            error instanceof Error
              ? error
              : new Error('워커 헬스체크 메시지 전송 실패');
          this.handleWorkerFailure(i, wrapped);
        }
      }
    }, this.healthCheckIntervalMs);
  }

  private clearPingTimeout(index: number): void {
    const timeout = this.pingTimeoutMap.get(index);
    if (!timeout) return;

    clearTimeout(timeout);
    this.pingTimeoutMap.delete(index);
  }

  private handleWorkerFailure(index: number, error: Error): void {
    if (this.shuttingDown) return;

    this.logger.error(`워커 ${index} 장애: ${error.message}`);
    this.clearPingTimeout(index);

    if (this.assignedTasks.has(index)) {
      if (this.onResult) {
        this.onResult(index, {
          success: false,
          error: `워커 장애로 작업이 실패했습니다: ${error.message}`,
        });
      } else {
        const taskData = this.assignedTasks.get(index);
        this.assignedTasks.delete(index);
        if (taskData) {
          this.decrementActiveByType(this.determineTaskType(taskData));
        }
        taskData?.task.reject(
          new Error(`워커 장애로 작업이 실패했습니다: ${error.message}`),
        );
      }
    }

    this.workerBusy[index] = false;

    const oldWorker = this.workerPool[index];
    if (oldWorker) {
      oldWorker.removeAllListeners();
      void oldWorker.terminate().catch(() => undefined);
    }

    this.createOrReplaceWorker(index);
  }
}
