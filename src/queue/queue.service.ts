import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { QueueTask, EnqueueOptions } from './interfaces/queue-task.interface';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { WorkerPoolService } from './worker-pool.service';
import { QueueOptionsParser } from './queue-options.parser';
import { QueueStatsService } from './queue-stats.service';
import { QueueSnapshotManager } from './queue-snapshot.manager';
import { QueueStateHolder } from './queue-state.holder';
import { QueueProcessorService } from './queue-processor.service';
import { readPositiveIntEnv } from './utils/env';

@Injectable()
export class QueueService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(QueueService.name);

  private readonly queueProcessIntervalMs = readPositiveIntEnv(
    'QUEUE_PROCESS_INTERVAL_MS',
    20,
  );
  private readonly batchAgingIntervalMs = 300;
  private readonly memoryCheckIntervalMs = readPositiveIntEnv(
    'QUEUE_MEMORY_CHECK_INTERVAL_MS',
    3000,
  );
  private readonly statsLogIntervalMs = readPositiveIntEnv(
    'QUEUE_STATS_LOG_INTERVAL_MS',
    60000,
  );
  private readonly snapshotIntervalMs = readPositiveIntEnv(
    'QUEUE_SNAPSHOT_INTERVAL_MS',
    30000,
  );

  private queueProcessTimer: ReturnType<typeof setInterval> | null = null;
  private batchAgingTimer: ReturnType<typeof setInterval> | null = null;
  private memoryCheckTimer: ReturnType<typeof setInterval> | null = null;
  private statsLogTimer: ReturnType<typeof setInterval> | null = null;
  private snapshotTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly state: QueueStateHolder,
    private readonly processor: QueueProcessorService,
    private readonly memoryService: MemoryService,
    private readonly batchService: BatchService,
    private readonly workerPoolService: WorkerPoolService,
    private readonly optionsParser: QueueOptionsParser,
    private readonly statsService: QueueStatsService,
    private readonly snapshotManager: QueueSnapshotManager,
  ) {}

  async onModuleInit() {
    const restored = await this.snapshotManager.restore();
    if (restored) {
      this.state.taskIdCounter = restored.taskIdCounter;
    }

    this.startProcessingIntervals();
    this.workerPoolService.initWorkerPool((workerId, result) => {
      this.processor.handleWorkerResult(workerId, result);
    });

    if (this.snapshotManager.enabled) {
      this.snapshotTimer = setInterval(() => {
        void this.saveSnapshot();
      }, this.snapshotIntervalMs);
      this.logger.log(
        `큐 영속성 활성화 (스냅샷 주기: ${this.snapshotIntervalMs}ms)`,
      );
    }

    this.logger.log('큐 시스템 초기화 완료');
  }

  async onModuleDestroy(): Promise<void> {
    if (this.queueProcessTimer) clearInterval(this.queueProcessTimer);
    if (this.batchAgingTimer) clearInterval(this.batchAgingTimer);
    if (this.memoryCheckTimer) clearInterval(this.memoryCheckTimer);
    if (this.statsLogTimer) clearInterval(this.statsLogTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    await this.saveSnapshot();
    await this.workerPoolService.destroy();
  }

  async enqueue<T>(
    execute: () => Promise<T>,
    options: EnqueueOptions = {},
  ): Promise<T> {
    const normalized = this.optionsParser.normalizeEnqueueOptions(
      options,
      this.state.executionTimeoutMs,
    );
    const { priority, category, size, timeout } = normalized;

    if (this.memoryService.memoryPressure && priority < 0) {
      this.statsService.incrementRejected();
      throw new Error('서버 과부하로 요청이 거부되었습니다.');
    }

    if (this.state.getTotalQueueLength() >= this.state.queueOverflowThreshold) {
      this.statsService.incrementRejected();
      throw new Error('큐가 가득 찼습니다. 잠시 후 다시 시도해주세요.');
    }

    return new Promise<T>((resolve, reject) => {
      const task: QueueTask = {
        id: ++this.state.taskIdCounter,
        requestId: normalized.requestId,
        execute: this.processor.withExecutionTimeout(execute, timeout),
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

      this.processor.wrapTaskWithQueueTimeout(task, this.state.taskTimeoutMs);

      if (
        normalized.batch &&
        this.batchService.shouldAddToBatch(task, this.state.batchQueues)
      ) {
        this.batchService.addTaskToBatch(
          task,
          this.state.batchQueues,
          this.processor.processBatch.bind(this.processor),
        );
      } else if (priority >= 5) {
        this.state.highPriorityQueue.push(task);
      } else if (priority >= 0) {
        this.state.normalPriorityQueue.push(task);
      } else {
        this.state.lowPriorityQueue.push(task);
      }

      this.processor.requestProcessQueue();
    });
  }

  getQueueStats() {
    const batchTaskCount = this.state.getBatchTaskCount();
    const recent = this.statsService.recent;

    return {
      highPriorityQueueLength: this.state.highPriorityQueue.length,
      normalPriorityQueueLength: this.state.normalPriorityQueue.length,
      lowPriorityQueueLength: this.state.lowPriorityQueue.length,
      batchQueueCount: this.state.batchQueues.size,
      batchTaskCount,
      totalQueueLength: this.state.getTotalQueueLength(),
      activeRequests: this.state.activeRequests,
      totalProcessed: this.statsService.totalProcessed,
      totalRejected: this.statsService.totalRejected,
      totalTimeout: this.statsService.totalTimeout,
      workloadGeneralQueueFallbackCount:
        this.optionsParser.workloadGeneralQueueFallbackCount,
      recentProcessed: recent.processed,
      recentRejected: recent.rejected,
      recentTimeout: recent.timeout,
      memoryPressure: this.memoryService.memoryPressure,
      workerPool: this.workerPoolService.getPoolStats(),
      persistence: {
        enabled: this.snapshotManager.enabled,
        snapshotIntervalMs: this.snapshotManager.enabled
          ? this.snapshotIntervalMs
          : null,
        lastSnapshotAt: this.snapshotManager.lastSnapshotAt,
      },
    };
  }

  private startProcessingIntervals(): void {
    this.queueProcessTimer = setInterval(() => {
      if (this.state.getTotalQueueLength() > 0) {
        this.processor.requestProcessQueue();
      }
    }, this.queueProcessIntervalMs);

    this.batchAgingTimer = setInterval(
      () =>
        this.batchService.processAgedBatches(
          this.state.batchQueues,
          this.processor.processBatch.bind(this.processor),
        ),
      this.batchAgingIntervalMs,
    );

    this.memoryCheckTimer = setInterval(() => {
      this.memoryService.checkMemoryUsage(
        () => {
          this.statsService.incrementTimeout(
            this.memoryService.cleanupOldTasks(
              {
                high: this.state.highPriorityQueue,
                normal: this.state.normalPriorityQueue,
                low: this.state.lowPriorityQueue,
              },
              this.state.batchQueues,
              this.state.taskTimeoutMs,
            ),
          );
        },
        () => {
          this.statsService.incrementRejected(
            this.memoryService.forceReduceQueues(
              this.state.lowPriorityQueue,
              this.state.batchQueues,
              this.processor.processBatch.bind(this.processor),
            ),
          );
        },
      );

      if (
        !this.memoryService.memoryPressure &&
        this.state.lowPriorityQueue.length > 0
      ) {
        this.processor.requestProcessQueue();
      }
    }, this.memoryCheckIntervalMs);

    this.statsLogTimer = setInterval(
      () =>
        this.statsService.logStats(
          this.state.activeRequests,
          this.state.getTotalQueueLength(),
          this.optionsParser.workloadGeneralQueueFallbackCount,
        ),
      this.statsLogIntervalMs,
    );
  }

  private async saveSnapshot(): Promise<void> {
    await this.snapshotManager.save(
      {
        high: this.state.highPriorityQueue,
        normal: this.state.normalPriorityQueue,
        low: this.state.lowPriorityQueue,
      },
      this.state.taskIdCounter,
    );
  }
}
