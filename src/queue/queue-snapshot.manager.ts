import { Injectable, Inject, Logger, Optional } from '@nestjs/common';
import {
  QueueTask,
  SerializedTask,
  QueueSnapshot,
} from './interfaces/queue-task.interface';
import { QueueStatsService } from './queue-stats.service';
import {
  QUEUE_PERSISTENCE,
  QueuePersistence,
} from './persistence/persistence.interface';
import { toErrorMessage } from './utils/error-message';

@Injectable()
export class QueueSnapshotManager {
  private readonly logger = new Logger(QueueSnapshotManager.name);
  lastSnapshotAt: number | null = null;

  constructor(
    private readonly statsService: QueueStatsService,
    @Optional()
    @Inject(QUEUE_PERSISTENCE)
    private readonly queuePersistence: QueuePersistence | null,
  ) {}

  get enabled(): boolean {
    return Boolean(this.queuePersistence);
  }

  serializeQueue(queue: QueueTask[]): SerializedTask[] {
    return queue.map((task) => ({
      id: task.id,
      requestId: task.requestId,
      timestamp: task.timestamp,
      priority: task.priority,
      category: task.category,
      size: task.size,
    }));
  }

  async save(
    queues: { high: QueueTask[]; normal: QueueTask[]; low: QueueTask[] },
    taskIdCounter: number,
  ): Promise<void> {
    if (!this.queuePersistence) return;

    try {
      await this.queuePersistence.saveSnapshot({
        timestamp: Date.now(),
        queues: {
          high: this.serializeQueue(queues.high),
          normal: this.serializeQueue(queues.normal),
          low: this.serializeQueue(queues.low),
        },
        stats: {
          totalProcessed: this.statsService.totalProcessed,
          totalRejected: this.statsService.totalRejected,
          totalTimeout: this.statsService.totalTimeout,
          taskIdCounter,
        },
      });
      this.lastSnapshotAt = Date.now();
    } catch (error) {
      this.logger.error(`큐 스냅샷 저장 실패: ${toErrorMessage(error)}`);
    }
  }

  async restore(): Promise<{ taskIdCounter: number } | null> {
    if (!this.queuePersistence) return null;

    try {
      const snapshot = await this.queuePersistence.loadSnapshot();
      if (!snapshot) return null;

      this.statsService.totalProcessed = snapshot.stats.totalProcessed;
      this.statsService.totalRejected = snapshot.stats.totalRejected;
      this.statsService.totalTimeout = snapshot.stats.totalTimeout;
      this.lastSnapshotAt = snapshot.timestamp;

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
        `큐 스냅샷 복구 완료 (processed=${this.statsService.totalProcessed}, rejected=${this.statsService.totalRejected}, timeout=${this.statsService.totalTimeout})`,
      );

      try {
        await this.queuePersistence.clearSnapshot();
      } catch (error) {
        this.logger.warn(
          `복구된 큐 스냅샷 정리 실패: ${toErrorMessage(error)}`,
        );
      }

      return { taskIdCounter: snapshot.stats.taskIdCounter };
    } catch (error) {
      this.logger.error(`큐 스냅샷 복구 실패: ${toErrorMessage(error)}`);
      return null;
    }
  }
}
