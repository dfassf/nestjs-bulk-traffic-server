import { Injectable, Logger } from '@nestjs/common';
import { Worker } from 'worker_threads';

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
export class WorkerHealthService {
  private readonly logger = new Logger(WorkerHealthService.name);
  private readonly healthCheckIntervalMs = 10000;
  private readonly healthCheckTimeoutMs = 2000;

  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private pingTimeoutMap = new Map<number, ReturnType<typeof setTimeout>>();

  startHealthChecks(
    workerPool: Array<Worker | null>,
    workerBusy: boolean[],
    onFailure: (index: number, error: Error) => void,
  ): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    this.healthCheckTimer = setInterval(() => {
      for (let i = 0; i < workerPool.length; i++) {
        const worker = workerPool[i];
        if (!worker) continue;
        if (workerBusy[i]) continue;
        if (this.pingTimeoutMap.has(i)) continue;

        try {
          worker.postMessage({ type: 'system', operation: 'ping' });

          const timeout = setTimeout(() => {
            onFailure(i, new Error('워커 헬스체크 타임아웃'));
          }, this.healthCheckTimeoutMs);

          this.pingTimeoutMap.set(i, timeout);
        } catch (error) {
          const wrapped =
            error instanceof Error
              ? error
              : new Error('워커 헬스체크 메시지 전송 실패');
          onFailure(i, wrapped);
        }
      }
    }, this.healthCheckIntervalMs);
  }

  clearPingTimeout(index: number): void {
    const timeout = this.pingTimeoutMap.get(index);
    if (!timeout) return;

    clearTimeout(timeout);
    this.pingTimeoutMap.delete(index);
  }

  destroy(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    for (const timer of this.pingTimeoutMap.values()) {
      clearTimeout(timer);
    }
    this.pingTimeoutMap.clear();
  }
}
