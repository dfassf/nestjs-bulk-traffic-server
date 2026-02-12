import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import {
  QueueTask,
  TaskBatch,
  EnqueueOptions,
  WorkerTaskData,
  WorkloadType,
  SerializedTask,
} from './interfaces/queue-task.interface';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { WorkerPoolService } from './worker-pool.service';
import {
  QUEUE_PERSISTENCE,
  QueuePersistence,
} from './persistence/persistence.interface';

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
  private readonly snapshotIntervalMs = this.readPositiveIntEnv(
    'QUEUE_SNAPSHOT_INTERVAL_MS',
    30000,
  );

  private taskIdCounter = 0;
  private activeRequests = 0;

  private totalProcessed = 0;
  private totalRejected = 0;
  private totalTimeout = 0;
  private workloadGeneralQueueFallbackCount = 0;

  private recentProcessed = 0;
  private recentRejected = 0;
  private recentTimeout = 0;

  private queueProcessTimer: ReturnType<typeof setInterval> | null = null;
  private batchAgingTimer: ReturnType<typeof setInterval> | null = null;
  private memoryCheckTimer: ReturnType<typeof setInterval> | null = null;
  private statsLogTimer: ReturnType<typeof setInterval> | null = null;
  private snapshotTimer: ReturnType<typeof setInterval> | null = null;
  private lastSnapshotAt: number | null = null;

  constructor(
    private readonly memoryService: MemoryService,
    private readonly batchService: BatchService,
    private readonly workerPoolService: WorkerPoolService,
    @Optional() @Inject(QUEUE_PERSISTENCE)
    private readonly queuePersistence: QueuePersistence | null,
  ) {}

  async onModuleInit() {
    await this.restoreSnapshot();
    this.startProcessingIntervals();
    this.workerPoolService.initWorkerPool((workerId, result) => {
      this.handleWorkerResult(workerId, result);
    });

    if (this.queuePersistence) {
      this.snapshotTimer = setInterval(() => {
        void this.saveSnapshot();
      }, this.snapshotIntervalMs);
      this.logger.log(`큐 영속성 활성화 (스냅샷 주기: ${this.snapshotIntervalMs}ms)`);
    }

    this.logger.log('큐 시스템 초기화 완료');
  }

  onModuleDestroy(): void {
    if (this.queueProcessTimer) clearInterval(this.queueProcessTimer);
    if (this.batchAgingTimer) clearInterval(this.batchAgingTimer);
    if (this.memoryCheckTimer) clearInterval(this.memoryCheckTimer);
    if (this.statsLogTimer) clearInterval(this.statsLogTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    void this.saveSnapshot();
    void this.workerPoolService.destroy();
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

  private serializeQueue(queue: QueueTask[]): SerializedTask[] {
    return queue.map((task) => ({
      id: task.id,
      requestId: task.requestId,
      timestamp: task.timestamp,
      priority: task.priority,
      category: task.category,
      size: task.size,
    }));
  }

  private async saveSnapshot(): Promise<void> {
    if (!this.queuePersistence) return;

    try {
      await this.queuePersistence.saveSnapshot({
        timestamp: Date.now(),
        queues: {
          high: this.serializeQueue(this.highPriorityQueue),
          normal: this.serializeQueue(this.normalPriorityQueue),
          low: this.serializeQueue(this.lowPriorityQueue),
        },
        stats: {
          totalProcessed: this.totalProcessed,
          totalRejected: this.totalRejected,
          totalTimeout: this.totalTimeout,
          taskIdCounter: this.taskIdCounter,
        },
      });
      this.lastSnapshotAt = Date.now();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`큐 스냅샷 저장 실패: ${message}`);
    }
  }

  private async restoreSnapshot(): Promise<void> {
    if (!this.queuePersistence) return;

    try {
      const snapshot = await this.queuePersistence.loadSnapshot();
      if (!snapshot) return;

      this.totalProcessed = snapshot.stats.totalProcessed;
      this.totalRejected = snapshot.stats.totalRejected;
      this.totalTimeout = snapshot.stats.totalTimeout;
      this.taskIdCounter = snapshot.stats.taskIdCounter;

      const queuedTaskCount =
        snapshot.queues.high.length +
        snapshot.queues.normal.length +
        snapshot.queues.low.length;
      if (queuedTaskCount > 0) {
        this.logger.warn(
          `스냅샷의 대기 작업 ${queuedTaskCount}개는 실행 함수가 없어 복구하지 않습니다.`,
        );
      }
      this.logger.log(
        `큐 스냅샷 복구 완료 (processed=${this.totalProcessed}, rejected=${this.totalRejected}, timeout=${this.totalTimeout})`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`큐 스냅샷 복구 실패: ${message}`);
    }
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

  private parsePositiveIntegerOption(
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

  private parsePriorityOption(value: unknown): number {
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

  private parseCategoryOption(value: unknown): string {
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

  private parseParamsOption(value: unknown): Record<string, unknown> {
    if (value === undefined || value === null) {
      return {};
    }

    if (typeof value !== 'object' || Array.isArray(value)) {
      throw new Error('params는 객체 형태여야 합니다.');
    }

    return value as Record<string, unknown>;
  }

  private parseFunctionCodeOption(value: unknown): string | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    if (typeof value !== 'string') {
      throw new Error('functionCode는 문자열이어야 합니다.');
    }

    return value;
  }

  private parseWorkloadTypeOption(value: unknown): WorkloadType | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    if (typeof value !== 'string') {
      throw new Error('workloadType은 문자열이어야 합니다.');
    }

    const normalized = value.trim().toLowerCase();
    if (normalized === WorkloadType.CPU) return WorkloadType.CPU;
    if (normalized === WorkloadType.MEMORY) return WorkloadType.MEMORY;
    if (normalized === WorkloadType.CUSTOM) return WorkloadType.CUSTOM;
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

  private normalizeEnqueueOptions(options: EnqueueOptions): {
    priority: number;
    category: string;
    size: number;
    timeout: number;
    workloadType?: WorkloadType;
    params: Record<string, unknown>;
    functionCode?: string;
    batch: boolean;
    requestId?: string | number;
  } {
    return {
      priority: this.parsePriorityOption(options.priority),
      category: this.parseCategoryOption(options.category),
      size: this.parsePositiveIntegerOption(options.size, 'size', 1),
      timeout: this.parsePositiveIntegerOption(
        options.timeout,
        'timeout',
        this.executionTimeoutMs,
      ),
      workloadType: this.parseWorkloadTypeOption(options.workloadType),
      params: this.parseParamsOption(options.params),
      functionCode: this.parseFunctionCodeOption(options.functionCode),
      batch: options.batch === true,
      requestId: options.requestId,
    };
  }

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

  private onWorkerTaskDispatchFailed = (): void => {
    this.activeRequests = Math.max(0, this.activeRequests - 1);
    this.incrementRejected();
    this.requestProcessQueue();
  };

  async enqueue<T>(
    execute: () => Promise<T>,
    options: EnqueueOptions = {},
  ): Promise<T> {
    const normalized = this.normalizeEnqueueOptions(options);
    const { priority, category, size, timeout } = normalized;

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
        requestId: normalized.requestId,
        execute: this.withExecutionTimeout(execute, timeout),
        resolve: resolve as (value: unknown) => void,
        reject,
        timestamp: Date.now(),
        priority,
        category,
        size,
        workloadType: normalized.workloadType,
        params: normalized.params,
        functionCode: normalized.functionCode,
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
        normalized.batch &&
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
    let workerDispatchCount = 0;

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

          if (this.isWorkerEligibleTask(task)) {
            this.activeRequests++;
            processed++;
            workerDispatchCount++;
            this.workerPoolService.addTask(this.createWorkerTaskData(task));
            continue;
          }

          this.activeRequests++;
          processed++;

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

      if (workerDispatchCount > 0) {
        this.workerPoolService.processWorkerTasks(this.onWorkerTaskDispatchFailed);
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

  private handleWorkerResult(
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
        this.incrementProcessed();
        this.activeRequests = Math.max(0, this.activeRequests - 1);
        this.requestProcessQueue();
      },
      () => {
        this.incrementRejected();
        this.activeRequests = Math.max(0, this.activeRequests - 1);
        this.requestProcessQueue();
      },
    );

    this.workerPoolService.processWorkerTasks(this.onWorkerTaskDispatchFailed);
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
      `[1분 통계] 처리: ${this.recentProcessed}, 거부: ${this.recentRejected}, 타임아웃: ${this.recentTimeout}, 활성: ${this.activeRequests}, 큐 길이: ${this.getTotalQueueLength()} / 누적 처리: ${this.totalProcessed}, unknown fallback: ${this.workloadGeneralQueueFallbackCount}`,
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
      workloadGeneralQueueFallbackCount: this.workloadGeneralQueueFallbackCount,
      recentProcessed: this.recentProcessed,
      recentRejected: this.recentRejected,
      recentTimeout: this.recentTimeout,
      memoryPressure: this.memoryService.memoryPressure,
      workerPool: this.workerPoolService.getPoolStats(),
      persistence: {
        enabled: Boolean(this.queuePersistence),
        snapshotIntervalMs: this.queuePersistence ? this.snapshotIntervalMs : null,
        lastSnapshotAt: this.lastSnapshotAt,
      },
    };
  }
}
