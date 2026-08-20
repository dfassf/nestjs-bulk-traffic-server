import { Injectable } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { WorkerBackend, WorkerBackendResult } from './interfaces/worker-backend.interface';
import { GoEngineClient } from './go-engine.client';

/**
 * Go 엔진 gRPC 클라이언트를 워커 백엔드 계약에 맞춘 어댑터.
 *
 * GoEngineClient 는 gRPC 응답 형태(GoEngineResult)를 그대로 돌려주는데,
 * 이 어댑터가 그것을 WorkerBackendResult 의 요청-응답(sync) 형태로 옮긴다.
 * 덕분에 라우터는 Kafka(보내고 끝냄)와 Go(응답 대기)를 같은 계약으로 다룬다.
 */
@Injectable()
export class GoEngineBackend implements WorkerBackend {
  readonly name = 'go';

  constructor(private readonly client: GoEngineClient) {}

  async execute(task: QueueTask): Promise<WorkerBackendResult> {
    const res = await this.client.execute(task);
    return {
      mode: 'sync',
      taskId: res.taskId,
      success: res.success,
      result: res.result,
      error: res.error || undefined,
      durationMs: res.durationMs,
      backend: this.name,
    };
  }

  async healthCheck(): Promise<boolean> {
    try {
      const res = await this.client.healthCheck();
      return Boolean(res?.healthy);
    } catch {
      return false;
    }
  }
}
