import { Injectable, Logger } from '@nestjs/common';
import { QueueTask, TaskBatch } from './interfaces/queue-task.interface';

@Injectable()
export class MemoryService {
  private readonly logger = new Logger(MemoryService.name);

  private readonly HIGH_MEMORY_THRESHOLD = 0.75;
  private readonly CRITICAL_MEMORY_THRESHOLD = 0.9;
  private readonly TASK_TIMEOUT_BUFFER_RATIO = 1.5;
  private readonly FORCE_REDUCE_BATCH_SIZE_THRESHOLD = 50;

  private _memoryPressure = false;

  get memoryPressure(): boolean {
    return this._memoryPressure;
  }

  checkMemoryUsage(
    cleanupCallback: () => void,
    forceReduceCallback: () => void,
  ): void {
    if (global.gc && typeof global.gc === 'function') {
      const memoryUsage = process.memoryUsage();
      const usageRatio = memoryUsage.heapUsed / memoryUsage.heapTotal;
      if (usageRatio > 0.7) global.gc();
    }

    const memoryUsage = process.memoryUsage();
    const usageRatio = memoryUsage.heapUsed / memoryUsage.heapTotal;

    const wasPressured = this._memoryPressure;
    this._memoryPressure = usageRatio > this.HIGH_MEMORY_THRESHOLD;

    if (this._memoryPressure && !wasPressured) {
      this.logger.warn(
        `높은 메모리 사용량 감지: ${Math.round(usageRatio * 100)}%`,
      );
    }

    if (!this._memoryPressure && wasPressured) {
      this.logger.log(`메모리 사용량 정상화: ${Math.round(usageRatio * 100)}%`);
    }

    if (this._memoryPressure) {
      cleanupCallback();

      if (usageRatio > this.CRITICAL_MEMORY_THRESHOLD && global.gc) {
        this.logger.warn('심각한 메모리 부족: 강제 GC 및 큐 정리 수행');
        global.gc();
        forceReduceCallback();
      }
    }
  }

  forceReduceQueues(
    lowPriorityQueue: QueueTask[],
    batchQueues: Map<string, TaskBatch>,
    processBatchCallback: (category: string) => void,
  ): number {
    let totalRejected = 0;

    if (lowPriorityQueue.length > 0) {
      const removeCount = Math.floor(lowPriorityQueue.length * 0.5);

      for (let i = 0; i < removeCount; i++) {
        const task = lowPriorityQueue.pop();
        if (task) {
          task.reject(new Error('서버 리소스 부족으로 요청이 취소되었습니다.'));
          totalRejected++;
        }
      }

      this.logger.warn(`메모리 부족으로 저우선순위 작업 ${totalRejected}개 제거`);
    }

    for (const [category, batch] of batchQueues.entries()) {
      if (batch.totalSize > this.FORCE_REDUCE_BATCH_SIZE_THRESHOLD) {
        processBatchCallback(category);
      }
    }

    return totalRejected;
  }

  cleanupOldTasks(
    queues: { high: QueueTask[]; normal: QueueTask[]; low: QueueTask[] },
    batchQueues: Map<string, TaskBatch>,
    taskTimeoutMs = 15000,
  ): number {
    const now = Date.now();
    const maxAge = taskTimeoutMs * this.TASK_TIMEOUT_BUFFER_RATIO;
    let totalTimeout = 0;

    const cleanupQueue = (queue: QueueTask[]) => {
      for (let i = queue.length - 1; i >= 0; i--) {
        const task = queue[i];
        if (now - task.timestamp > maxAge) {
          queue.splice(i, 1);
          task.reject(new Error('요청 처리 지연으로 취소되었습니다.'));
          totalTimeout++;
        }
      }
    };

    cleanupQueue(queues.high);
    cleanupQueue(queues.normal);
    cleanupQueue(queues.low);

    for (const [category, batch] of batchQueues.entries()) {
      if (now - batch.createdAt > maxAge) {
        for (const task of batch.tasks) {
          task.reject(new Error('배치 처리 시간 초과'));
          totalTimeout++;
        }
        batchQueues.delete(category);
      }
    }

    return totalTimeout;
  }
}
