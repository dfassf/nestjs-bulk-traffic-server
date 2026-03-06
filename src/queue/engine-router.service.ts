import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { GoEngineClient, GoEngineResult } from './go-engine.client';

export type WorkerEngine = 'node' | 'go' | 'both';

@Injectable()
export class EngineRouterService implements OnModuleInit {
  private readonly logger = new Logger(EngineRouterService.name);
  private readonly engine: WorkerEngine;

  constructor(
    private readonly goEngineClient: GoEngineClient,
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

  async dispatchToGo(task: QueueTask): Promise<GoEngineResult> {
    return this.goEngineClient.execute(task);
  }
}
