import { Injectable } from '@nestjs/common';
import { QueueTask, TaskBatch } from './interfaces/queue-task.interface';
import { readPositiveIntEnv } from './utils/env';

@Injectable()
export class QueueStateHolder {
  readonly highPriorityQueue: QueueTask[] = [];
  readonly normalPriorityQueue: QueueTask[] = [];
  readonly lowPriorityQueue: QueueTask[] = [];
  readonly batchQueues: Map<string, TaskBatch> = new Map();

  readonly concurrentTasks = readPositiveIntEnv('QUEUE_CONCURRENT_TASKS', 50);
  readonly maxConcurrentRequests = readPositiveIntEnv('QUEUE_MAX_CONCURRENT_REQUESTS', 200);
  readonly taskTimeoutMs = readPositiveIntEnv('QUEUE_TASK_TIMEOUT_MS', 15000);
  readonly executionTimeoutMs = readPositiveIntEnv('QUEUE_EXECUTION_TIMEOUT_MS', 10000);
  readonly queueOverflowThreshold = readPositiveIntEnv('QUEUE_OVERFLOW_THRESHOLD', 3000);

  taskIdCounter = 0;
  activeRequests = 0;
  isProcessingQueue = false;
  processSignalPending = false;

  get allQueues(): QueueTask[][] {
    return [this.highPriorityQueue, this.normalPriorityQueue, this.lowPriorityQueue];
  }

  getBatchTaskCount(): number {
    let count = 0;
    for (const batch of this.batchQueues.values()) {
      count += batch.tasks.length;
    }
    return count;
  }

  getTotalQueueLength(): number {
    return (
      this.highPriorityQueue.length +
      this.normalPriorityQueue.length +
      this.lowPriorityQueue.length +
      this.getBatchTaskCount()
    );
  }

  decrementActiveRequests(): void {
    this.activeRequests = Math.max(0, this.activeRequests - 1);
  }

  removeTaskFromQueues(task: QueueTask): boolean {
    for (const queue of this.allQueues) {
      const index = queue.indexOf(task);
      if (index !== -1) {
        queue.splice(index, 1);
        return true;
      }
    }

    for (const [, batch] of this.batchQueues.entries()) {
      const index = batch.tasks.indexOf(task);
      if (index !== -1) {
        const [removed] = batch.tasks.splice(index, 1);
        batch.totalSize -= removed.size ?? 1;

        if (batch.tasks.length === 0) {
          this.batchQueues.delete(batch.category);
        }

        return true;
      }
    }

    return false;
  }
}
