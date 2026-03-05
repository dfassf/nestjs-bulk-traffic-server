import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { WorkerPoolService } from './worker-pool.service';
import { GoEngineClient, GoEngineResult } from './go-engine.client';
import { BenchmarkService } from './benchmark.service';

export type WorkerEngine = 'node' | 'go' | 'both';

@Injectable()
export class EngineRouterService implements OnModuleInit {
  private readonly logger = new Logger(EngineRouterService.name);
  private readonly engine: WorkerEngine;

  constructor(
    private readonly workerPoolService: WorkerPoolService,
    private readonly goEngineClient: GoEngineClient,
    private readonly benchmarkService: BenchmarkService,
  ) {
    const env = (process.env.WORKER_ENGINE || 'node').toLowerCase();
    if (env === 'go' || env === 'both') {
      this.engine = env as WorkerEngine;
    } else {
      this.engine = 'node';
    }
  }

  onModuleInit() {
    this.logger.log(`엔진 모드: ${this.engine}`);
    if (this.engine === 'go' || this.engine === 'both') {
      this.logger.log('Go 엔진 사이드카 활성화');
    }
  }

  getEngine(): WorkerEngine {
    return this.engine;
  }

  isGoEnabled(): boolean {
    return this.engine === 'go' || this.engine === 'both';
  }

  async dispatchToGo(task: QueueTask): Promise<GoEngineResult> {
    return this.goEngineClient.execute(task);
  }

  async dispatchBoth(
    task: QueueTask,
    nodeExecute: () => Promise<unknown>,
  ): Promise<{ winner: 'node' | 'go'; result: unknown }> {
    const nodeStart = Date.now();
    const goStart = Date.now();

    const [nodeSettled, goSettled] = await Promise.allSettled([
      nodeExecute().then((r) => ({ result: r, durationMs: Date.now() - nodeStart })),
      this.goEngineClient.execute(task).then((r) => ({
        result: r.result,
        durationMs: r.durationMs,
        success: r.success,
        error: r.error,
      })),
    ]);

    this.benchmarkService.record(
      task,
      nodeSettled,
      goSettled,
    );

    // Return first successful result, preferring faster one
    if (nodeSettled.status === 'fulfilled' && goSettled.status === 'fulfilled') {
      const nodeMs = nodeSettled.value.durationMs;
      const goMs = goSettled.value.durationMs;
      const winner = nodeMs <= goMs ? 'node' : 'go';
      return {
        winner,
        result: winner === 'node' ? nodeSettled.value.result : goSettled.value.result,
      };
    }

    if (nodeSettled.status === 'fulfilled') {
      return { winner: 'node', result: nodeSettled.value.result };
    }

    if (goSettled.status === 'fulfilled') {
      return { winner: 'go', result: goSettled.value.result };
    }

    throw (nodeSettled as PromiseRejectedResult).reason;
  }
}
