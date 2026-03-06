import { Injectable } from '@nestjs/common';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { WorkerPoolService } from './worker-pool.service';
import { QueueStatsService } from './queue-stats.service';
import { QueueStateHolder } from './queue-state.holder';

@Injectable()
export class QueueProcessorService {
  constructor(
    private readonly state: QueueStateHolder,
    private readonly memoryService: MemoryService,
    private readonly batchService: BatchService,
    private readonly workerPoolService: WorkerPoolService,
    private readonly statsService: QueueStatsService,
  ) {}

  requestProcessQueue(): void {
    if (this.state.processSignalPending) return;

    this.state.processSignalPending = true;
    setImmediate(() => {
      this.state.processSignalPending = false;
      this.processQueue();
    });
  }

  processQueue(): void {
    if (this.state.isProcessingQueue) return;
    if (this.state.activeRequests >= this.state.maxConcurrentRequests) return;

    this.state.isProcessingQueue = true;
    let processed = 0;
    let workerDispatchCount = 0;

    try {
      const availableSlots = Math.min(
        this.state.concurrentTasks,
        this.state.maxConcurrentRequests - this.state.activeRequests,
      );

      const queues = this.memoryService.memoryPressure
        ? [this.state.highPriorityQueue, this.state.normalPriorityQueue]
        : [
            this.state.highPriorityQueue,
            this.state.normalPriorityQueue,
            this.state.lowPriorityQueue,
          ];

      for (const queue of queues) {
        while (queue.length > 0 && processed < availableSlots) {
          const task = queue.shift();
          if (!task) continue;

          if (this.isWorkerEligibleTask(task)) {
            this.state.activeRequests++;
            processed++;
            workerDispatchCount++;
            this.workerPoolService.addTask(this.createWorkerTaskData(task));
            continue;
          }

          this.state.activeRequests++;
          processed++;

          Promise.resolve()
            .then(() => task.execute())
            .then((result) => {
              task.resolve(result);
              this.statsService.incrementProcessed();
            })
            .catch((error) => {
              task.reject(error);
            })
            .finally(() => {
              this.state.decrementActiveRequests();
              this.requestProcessQueue();
            });
        }

        if (processed >= availableSlots) break;
      }

      if (workerDispatchCount > 0) {
        this.workerPoolService.processWorkerTasks(this.onWorkerTaskDispatchFailed);
      }
    } finally {
      this.state.isProcessingQueue = false;

      if (
        this.state.activeRequests < this.state.maxConcurrentRequests &&
        this.state.getTotalQueueLength() > 0
      ) {
        this.requestProcessQueue();
      }
    }
  }

  processBatch(category: string): void {
    this.batchService.processBatch(
      category,
      this.state.batchQueues,
      this.state.activeRequests,
      this.state.maxConcurrentRequests,
      (processed, activeChange) => {
        this.statsService.incrementProcessed(processed);
        this.state.activeRequests = Math.max(0, this.state.activeRequests + activeChange);

        if (activeChange < 0) {
          this.requestProcessQueue();
        }
      },
    );
  }

  handleWorkerResult(
    workerId: number,
    result: {
      success?: boolean;
      result?: unknown;
      error?: string;
    },
  ): void {
    this.workerPoolService.handleWorkerResult(
      workerId,
      result,
      () => {
        this.statsService.incrementProcessed();
        this.state.decrementActiveRequests();
        this.requestProcessQueue();
      },
      () => {
        this.statsService.incrementRejected();
        this.state.decrementActiveRequests();
        this.requestProcessQueue();
      },
    );

    this.workerPoolService.processWorkerTasks(this.onWorkerTaskDispatchFailed);
  }

  withExecutionTimeout<T>(
    execute: () => Promise<T>,
    timeoutMs: number,
  ): () => Promise<T> {
    return () =>
      new Promise<T>((resolve, reject) => {
        let settled = false;

        const timerId = setTimeout(() => {
          if (settled) return;
          settled = true;
          reject(new Error(`작업 실행 시간 초과 (${timeoutMs}ms)`));
        }, timeoutMs);

        Promise.resolve()
          .then(execute)
          .then((result) => {
            if (settled) return;
            settled = true;
            clearTimeout(timerId);
            resolve(result);
          })
          .catch((error) => {
            if (settled) return;
            settled = true;
            clearTimeout(timerId);
            reject(error);
          });
      });
  }

  private readonly onWorkerTaskDispatchFailed = (): void => {
    this.state.decrementActiveRequests();
    this.statsService.incrementRejected();
    this.requestProcessQueue();
  };

  private isWorkerEligibleTask(task: QueueTask): boolean {
    if (!this.workerPoolService.isEnabled) return false;
    if (task.priority < 0) return false;
    return (
      task.workloadType === WorkloadType.CPU ||
      task.workloadType === WorkloadType.MEMORY ||
      task.workloadType === WorkloadType.CUSTOM
    );
  }

  private createWorkerTaskData(task: QueueTask): WorkerTaskData {
    return {
      task,
      type: task.workloadType,
      params: task.params || {},
      functionCode: task.functionCode,
      timeout: task.timeout,
    };
  }
}
