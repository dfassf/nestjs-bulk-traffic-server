import { Injectable, Logger } from '@nestjs/common';
import { QueueTask, TaskBatch } from './interfaces/queue-task.interface';

@Injectable()
export class BatchService {
  private readonly logger = new Logger(BatchService.name);

  private readonly BATCH_SIZE_THRESHOLD = 10;
  private readonly BATCH_MAX_AGE_MS = 1500;
  private readonly BATCH_MAX_SIZE = 150;
  private readonly BATCH_ELIGIBLE_SIZE_MAX = 3;

  shouldAddToBatch(
    task: QueueTask,
    batchQueues: Map<string, TaskBatch>,
  ): boolean {
    if (task.priority >= 5) return false;
    if (!task.category) return false;
    if (batchQueues.has(task.category)) return true;

    return (task.size ?? 1) <= this.BATCH_ELIGIBLE_SIZE_MAX;
  }

  addTaskToBatch(
    task: QueueTask,
    batchQueues: Map<string, TaskBatch>,
    processBatchCallback: (category: string) => void,
  ): void {
    let batch = batchQueues.get(task.category);

    if (!batch) {
      batch = {
        tasks: [],
        category: task.category,
        totalSize: 0,
        createdAt: Date.now(),
      };
      batchQueues.set(task.category, batch);
    }

    batch.tasks.push(task);
    batch.totalSize += task.size || 1;

    if (
      batch.tasks.length >= this.BATCH_SIZE_THRESHOLD ||
      batch.totalSize >= this.BATCH_MAX_SIZE
    ) {
      this.logger.debug(`배치 임계치 도달: ${task.category}`);
      setImmediate(() => processBatchCallback(task.category));
    }
  }

  processAgedBatches(
    batchQueues: Map<string, TaskBatch>,
    processBatchCallback: (category: string) => void,
  ): void {
    const now = Date.now();

    for (const [category, batch] of batchQueues.entries()) {
      if (now - batch.createdAt >= this.BATCH_MAX_AGE_MS) {
        processBatchCallback(category);
      }
    }
  }

  processBatch(
    category: string,
    batchQueues: Map<string, TaskBatch>,
    activeRequests: number,
    maxConcurrentRequests: number,
    onComplete: (processed: number, activeChange: number) => void,
  ): void {
    const batch = batchQueues.get(category);
    if (!batch || batch.tasks.length === 0) {
      batchQueues.delete(category);
      return;
    }

    if (activeRequests >= maxConcurrentRequests) return;

    batchQueues.delete(category);

    let processed = 0;
    Promise.all(
      batch.tasks.map((task) => {
        return Promise.resolve()
          .then(() => task.execute())
          .then((result) => {
            task.resolve(result);
            processed++;
          })
          .catch((error) => {
            task.reject(error);
          });
      }),
    ).finally(() => {
      onComplete(processed, -1);
    });

    onComplete(0, 1);
  }
}
