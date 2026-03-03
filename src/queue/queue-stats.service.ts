import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class QueueStatsService {
  private readonly logger = new Logger(QueueStatsService.name);

  totalProcessed = 0;
  totalRejected = 0;
  totalTimeout = 0;

  private recentProcessed = 0;
  private recentRejected = 0;
  private recentTimeout = 0;

  incrementProcessed(count = 1): void {
    this.totalProcessed += count;
    this.recentProcessed += count;
  }

  incrementRejected(count = 1): void {
    this.totalRejected += count;
    this.recentRejected += count;
  }

  incrementTimeout(count = 1): void {
    this.totalTimeout += count;
    this.recentTimeout += count;
  }

  get recent() {
    return {
      processed: this.recentProcessed,
      rejected: this.recentRejected,
      timeout: this.recentTimeout,
    };
  }

  logStats(
    activeRequests: number,
    totalQueueLength: number,
    workloadGeneralQueueFallbackCount: number,
  ): void {
    this.logger.log(
      `[1분 통계] 처리: ${this.recentProcessed}, 거부: ${this.recentRejected}, 타임아웃: ${this.recentTimeout}, 활성: ${activeRequests}, 큐 길이: ${totalQueueLength} / 누적 처리: ${this.totalProcessed}, unknown fallback: ${workloadGeneralQueueFallbackCount}`,
    );

    this.recentProcessed = 0;
    this.recentRejected = 0;
    this.recentTimeout = 0;
  }
}
