import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Worker } from 'worker_threads';
import * as path from 'path';
import { existsSync } from 'fs';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';
import { WorkerHealthService } from './worker-health.service';
import { WorkerTaskRouterService } from './worker-task-router.service';
import { readPositiveIntEnv } from './utils/env';

interface WorkerMessage {
  success?: boolean;
  result?: unknown;
  error?: string;
  initialized?: boolean;
  healthCheck?: boolean;
  operation?: string;
  duration?: number;
}

@Injectable()
export class WorkerPoolService implements OnModuleDestroy {
  private readonly logger = new Logger(WorkerPoolService.name);

  private readonly useWorkers =
    process.env.DISABLE_WORKERS !== 'true' && process.env.NODE_ENV !== 'test';
  private readonly workerCount = readPositiveIntEnv('WORKER_POOL_SIZE', 4);

  private workerPool: Array<Worker | null> = [];
  private workerBusy: boolean[] = [];
  private workerTaskQueue: WorkerTaskData[] = [];
  private assignedTasks = new Map<number, WorkerTaskData>();

  private onResult: ((workerId: number, result: WorkerMessage) => void) | null =
    null;
  private shuttingDown = false;

  constructor(
    private readonly healthService: WorkerHealthService,
    private readonly router: WorkerTaskRouterService,
  ) {}

  get isEnabled(): boolean {
    return this.useWorkers;
  }

  get taskQueue(): WorkerTaskData[] {
    return this.workerTaskQueue;
  }

  getPoolStats() {
    const totalWorkers = this.workerPool.filter(Boolean).length;
    const busyWorkers = this.workerBusy.filter(Boolean).length;

    return {
      totalWorkers,
      busyWorkers,
      idleWorkers: Math.max(0, totalWorkers - busyWorkers),
      pendingTasks: this.workerTaskQueue.length,
      activeByType: this.router.getActiveByType(),
      queueByType: this.router.getPendingQueueByType(this.workerTaskQueue),
      limitsByType: this.router.limits,
      dispatchedByType: this.router.getDispatchedByType(),
      droppedUnknownWorkloadCount: this.router.droppedUnknownWorkloadCount,
      enabled: this.useWorkers,
    };
  }

  initWorkerPool(
    onResult: (workerId: number, result: WorkerMessage) => void,
  ): void {
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

    this.healthService.startHealthChecks(
      this.workerPool,
      this.workerBusy,
      (index, error) => this.handleWorkerFailure(index, error),
    );
    this.logger.log(`워커 풀 초기화 완료: ${this.workerCount}개`);
  }

  onModuleDestroy(): void {
    void this.destroy();
  }

  async destroy(): Promise<void> {
    this.shuttingDown = true;
    this.healthService.destroy();

    const terminations: Array<Promise<number>> = [];
    for (const worker of this.workerPool) {
      if (worker) terminations.push(worker.terminate());
    }

    await Promise.allSettled(terminations);
    this.workerPool = [];
    this.workerBusy = [];
    this.workerTaskQueue = [];
    this.assignedTasks.clear();
    this.router.resetActiveByType();
    this.onResult = null;
  }

  addTask(taskData: WorkerTaskData): void {
    this.workerTaskQueue.push(taskData);
  }

  processWorkerTasks(onTaskFailed: (task: QueueTask) => void): void {
    if (!this.useWorkers || this.workerTaskQueue.length === 0) return;

    for (let i = 0; i < this.workerPool.length; i++) {
      const worker = this.workerPool[i];
      if (!worker || this.workerBusy[i]) continue;
      if (this.workerTaskQueue.length === 0) break;

      const picked = this.router.dequeueDispatchableTask(
        this.workerTaskQueue,
        onTaskFailed,
      );
      if (!picked) continue;
      const { taskData, type } = picked;

      try {
        worker.postMessage({
          type,
          operation: this.router.determineOperation(taskData),
          params: taskData.params || {},
          functionCode: taskData.functionCode ?? taskData.task.functionCode,
          timeout: taskData.timeout ?? taskData.task.timeout,
        });

        this.workerBusy[i] = true;
        this.router.incrementActiveByType(type);
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
    this.router.decrementActiveByType(this.router.determineTaskType(taskData));

    if (result.success) {
      taskData.task.resolve(result.result);
      onSuccess(taskData.task);
      return;
    }

    taskData.task.reject(new Error(result.error || '워커 처리 실패'));
    onFailed(taskData.task);
  }

  // keep for backward compat with tests
  determineTaskType(taskData: WorkerTaskData): WorkloadType {
    return this.router.determineTaskType(taskData);
  }

  determineOperation(taskData: WorkerTaskData): string {
    return this.router.determineOperation(taskData);
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
        this.healthService.clearPingTimeout(index);
        return;
      }
      this.healthService.clearPingTimeout(index);
      this.onResult?.(index, result);
    });

    worker.on('error', (error) => this.handleWorkerFailure(index, error));

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

  private handleWorkerFailure(index: number, error: Error): void {
    if (this.shuttingDown) return;

    this.logger.error(`워커 ${index} 장애: ${error.message}`);
    this.healthService.clearPingTimeout(index);

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
          this.router.decrementActiveByType(
            this.router.determineTaskType(taskData),
          );
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
