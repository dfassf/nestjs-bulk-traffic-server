import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import {
  QueueTask,
  TaskBatch,
  EnqueueOptions,
} from './interfaces/queue-task.interface';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';

@Injectable()
export class QueueService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(QueueService.name);

  private highPriorityQueue: QueueTask[] = [];
  private normalPriorityQueue: QueueTask[] = [];
  private lowPriorityQueue: QueueTask[] = [];
  private batchQueues: Map<string, TaskBatch> = new Map();

  private isProcessingQueue = false;
  private processSignalPending = false;

  private readonly concurrentTasks = this.readPositiveIntEnv(
    'QUEUE_CONCURRENT_TASKS',
    50,
  );
  private readonly maxConcurrentRequests = this.readPositiveIntEnv(
    'QUEUE_MAX_CONCURRENT_REQUESTS',
    200,
  );
  private readonly taskTimeoutMs = this.readPositiveIntEnv(
    'QUEUE_TASK_TIMEOUT_MS',
    15000,
  );
  private readonly executionTimeoutMs = this.readPositiveIntEnv(
    'QUEUE_EXECUTION_TIMEOUT_MS',
    10000,
  );
  private readonly queueOverflowThreshold = this.readPositiveIntEnv(
    'QUEUE_OVERFLOW_THRESHOLD',
    3000,
  );

  private readonly queueProcessIntervalMs = this.readPositiveIntEnv(
    'QUEUE_PROCESS_INTERVAL_MS',
    20,
  );
  private readonly batchAgingIntervalMs = 300;
  private readonly memoryCheckIntervalMs = this.readPositiveIntEnv(
    'QUEUE_MEMORY_CHECK_INTERVAL_MS',
    3000,
  );
  private readonly statsLogIntervalMs = this.readPositiveIntEnv(
    'QUEUE_STATS_LOG_INTERVAL_MS',
    60000,
  );

  private taskIdCounter = 0;
  private activeRequests = 0;

  private totalProcessed = 0;
  private totalRejected = 0;
  private totalTimeout = 0;

  private recentProcessed = 0;
  private recentRejected = 0;
  private recentTimeout = 0;

  private queueProcessTimer: ReturnType<typeof setInterval> | null = null;
  private batchAgingTimer: ReturnType<typeof setInterval> | null = null;
  private memoryCheckTimer: ReturnType<typeof setInterval> | null = null;
  private statsLogTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly memoryService: MemoryService,
    private readonly batchService: BatchService,
  ) {}

  async onModuleInit() {
    this.startProcessingIntervals();
    this.logger.log('큐 시스템 초기화 완료');
  }

  onModuleDestroy(): void {
    if (this.queueProcessTimer) clearInterval(this.queueProcessTimer);
    if (this.batchAgingTimer) clearInterval(this.batchAgingTimer);
    if (this.memoryCheckTimer) clearInterval(this.memoryCheckTimer);
    if (this.statsLogTimer) clearInterval(this.statsLogTimer);
  }

  private readPositiveIntEnv(name: string, fallback: number): number {
    const raw = process.env[name];
    if (!raw) return fallback;

    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  private startProcessingIntervals(): void {
    // 이벤트 기반 처리 실패 시를 대비한 fallback 폴링
    this.queueProcessTimer = setInterval(() => {
      if (this.getTotalQueueLength() > 0) {
        this.requestProcessQueue();
      }
    }, this.queueProcessIntervalMs);

    this.batchAgingTimer = setInterval(
      () =>
        this.batchService.processAgedBatches(
          this.batchQueues,
          this.processBatch.bind(this),
        ),
      this.batchAgingIntervalMs,
    );

    this.memoryCheckTimer = setInterval(() => {
      this.memoryService.checkMemoryUsage(
        () => {
          this.incrementTimeout(
            this.memoryService.cleanupOldTasks(
              {
                high: this.highPriorityQueue,
                normal: this.normalPriorityQueue,
                low: this.lowPriorityQueue,
              },
              this.batchQueues,
              this.taskTimeoutMs,
            ),
          );
        },
        () => {
          this.incrementRejected(
            this.memoryService.forceReduceQueues(
              this.lowPriorityQueue,
              this.batchQueues,
              this.processBatch.bind(this),
            ),
          );
        },
      );

      // 메모리 압박이 해제되면 대기 중이던 저우선순위 큐 처리를 재개한다.
      if (!this.memoryService.memoryPressure && this.lowPriorityQueue.length > 0) {
        this.requestProcessQueue();
      }
    }, this.memoryCheckIntervalMs);

    this.statsLogTimer = setInterval(
      () => this.logStats(),
      this.statsLogIntervalMs,
    );
  }

  private requestProcessQueue(): void {
    if (this.processSignalPending) return;

    this.processSignalPending = true;
    setImmediate(() => {
      this.processSignalPending = false;
      this.processQueue();
    });
  }

  private incrementProcessed(count = 1): void {
    this.totalProcessed += count;
    this.recentProcessed += count;
  }

  private incrementRejected(count = 1): void {
    this.totalRejected += count;
    this.recentRejected += count;
  }

  private incrementTimeout(count = 1): void {
    this.totalTimeout += count;
    this.recentTimeout += count;
  }

  private getBatchTaskCount(): number {
    let batchTaskCount = 0;

    for (const batch of this.batchQueues.values()) {
      batchTaskCount += batch.tasks.length;
    }

    return batchTaskCount;
  }

  private getTotalQueueLength(): number {
    return (
      this.highPriorityQueue.length +
      this.normalPriorityQueue.length +
      this.lowPriorityQueue.length +
      this.getBatchTaskCount()
    );
  }

  private withExecutionTimeout<T>(
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

  async enqueue<T>(
    execute: () => Promise<T>,
    options: EnqueueOptions = {},
  ): Promise<T> {
    const priority = options.priority ?? 0;
    const category = options.category || 'default';
    const size = options.size ?? 1;
    const timeout = options.timeout ?? this.executionTimeoutMs;

    if (this.memoryService.memoryPressure && priority < 0) {
      this.incrementRejected();
      throw new Error('서버 과부하로 요청이 거부되었습니다.');
    }

    if (this.getTotalQueueLength() >= this.queueOverflowThreshold) {
      this.incrementRejected();
      throw new Error('큐가 가득 찼습니다. 잠시 후 다시 시도해주세요.');
    }

    return new Promise<T>((resolve, reject) => {
      const task: QueueTask = {
        id: ++this.taskIdCounter,
        requestId: options.requestId,
        execute: this.withExecutionTimeout(execute, timeout),
        resolve: resolve as (value: unknown) => void,
        reject,
        timestamp: Date.now(),
        priority,
        category,
        size,
        timeout,
      };

      const timeoutId = setTimeout(() => {
        if (this.removeTaskFromQueues(task)) {
          this.incrementTimeout();
          reject(new Error('큐 대기 시간 초과'));
        }
      }, this.taskTimeoutMs);

      const originalResolve = task.resolve;
      const originalReject = task.reject;

      task.resolve = (value: unknown) => {
        clearTimeout(timeoutId);
        originalResolve(value);
      };

      task.reject = (reason?: Error | string) => {
        clearTimeout(timeoutId);
        originalReject(reason);
      };

      if (
        options.batch &&
        this.batchService.shouldAddToBatch(task, this.batchQueues)
      ) {
        this.batchService.addTaskToBatch(
          task,
          this.batchQueues,
          this.processBatch.bind(this),
        );
      } else if (priority >= 5) {
        this.highPriorityQueue.push(task);
      } else if (priority >= 0) {
        this.normalPriorityQueue.push(task);
      } else {
        this.lowPriorityQueue.push(task);
      }

      this.requestProcessQueue();
    });
  }

  private processQueue(): void {
    if (this.isProcessingQueue) return;
    if (this.activeRequests >= this.maxConcurrentRequests) return;

    this.isProcessingQueue = true;
    let processed = 0;

    try {
      const availableSlots = Math.min(
        this.concurrentTasks,
        this.maxConcurrentRequests - this.activeRequests,
      );

      const queues = this.memoryService.memoryPressure
        ? [this.highPriorityQueue, this.normalPriorityQueue]
        : [
            this.highPriorityQueue,
            this.normalPriorityQueue,
            this.lowPriorityQueue,
          ];

      for (const queue of queues) {
        while (queue.length > 0 && processed < availableSlots) {
          const task = queue.shift();
          if (!task) continue;

          processed++;
          this.activeRequests++;

          Promise.resolve()
            .then(() => task.execute())
            .then((result) => {
              task.resolve(result);
              this.incrementProcessed();
            })
            .catch((error) => {
              task.reject(error);
            })
            .finally(() => {
              this.activeRequests = Math.max(0, this.activeRequests - 1);
              this.requestProcessQueue();
            });
        }

        if (processed >= availableSlots) break;
      }
    } finally {
      this.isProcessingQueue = false;

      if (
        this.activeRequests < this.maxConcurrentRequests &&
        this.getTotalQueueLength() > 0
      ) {
        this.requestProcessQueue();
      }
    }
  }

  private processBatch(category: string): void {
    this.batchService.processBatch(
      category,
      this.batchQueues,
      this.activeRequests,
      this.maxConcurrentRequests,
      (processed, activeChange) => {
        this.incrementProcessed(processed);
        this.activeRequests = Math.max(0, this.activeRequests + activeChange);

        if (activeChange < 0) {
          this.requestProcessQueue();
        }
      },
    );
  }

  private removeTaskFromQueues(task: QueueTask): boolean {
    const queues = [
      this.highPriorityQueue,
      this.normalPriorityQueue,
      this.lowPriorityQueue,
    ];

    for (const queue of queues) {
      const index = queue.indexOf(task);
      if (index !== -1) {
        queue.splice(index, 1);
        return true;
      }
    }

    for (const [category, batch] of this.batchQueues.entries()) {
      const index = batch.tasks.indexOf(task);
      if (index !== -1) {
        const [removed] = batch.tasks.splice(index, 1);
        batch.totalSize -= removed.size ?? 1;

        if (batch.tasks.length === 0) {
          this.batchQueues.delete(category);
        }

        return true;
      }
    }

    return false;
  }

  private logStats(): void {
    this.logger.log(
      `[1분 통계] 처리: ${this.recentProcessed}, 거부: ${this.recentRejected}, 타임아웃: ${this.recentTimeout}, 활성: ${this.activeRequests}, 큐 길이: ${this.getTotalQueueLength()} / 누적 처리: ${this.totalProcessed}`,
    );

    this.recentProcessed = 0;
    this.recentRejected = 0;
    this.recentTimeout = 0;
  }

  getQueueStats() {
    const batchTaskCount = this.getBatchTaskCount();

    return {
      highPriorityQueueLength: this.highPriorityQueue.length,
      normalPriorityQueueLength: this.normalPriorityQueue.length,
      lowPriorityQueueLength: this.lowPriorityQueue.length,
      batchQueueCount: this.batchQueues.size,
      batchTaskCount,
      totalQueueLength: this.getTotalQueueLength(),
      activeRequests: this.activeRequests,
      totalProcessed: this.totalProcessed,
      totalRejected: this.totalRejected,
      totalTimeout: this.totalTimeout,
      recentProcessed: this.recentProcessed,
      recentRejected: this.recentRejected,
      recentTimeout: this.recentTimeout,
      memoryPressure: this.memoryService.memoryPressure,
    };
  }
}
