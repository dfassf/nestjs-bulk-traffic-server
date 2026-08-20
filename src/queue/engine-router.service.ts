import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { GoEngineClient, GoEngineResult } from './go-engine.client';
import { KafkaProducerBackend } from './kafka-producer.backend';
import { WorkerBackendResult } from './interfaces/worker-backend.interface';
import { readWorkerEngineEnv, WorkerEngine } from './utils/env';

export { WorkerEngine };

@Injectable()
export class EngineRouterService implements OnModuleInit {
  private readonly logger = new Logger(EngineRouterService.name);
  private readonly engine: WorkerEngine;

  constructor(
    private readonly goEngineClient: GoEngineClient,
    private readonly kafkaProducerBackend: KafkaProducerBackend,
  ) {
    this.engine = readWorkerEngineEnv();
  }

  onModuleInit() {
    this.logger.log(`엔진 모드: ${this.engine}`);
    if (this.engine === 'go' || this.engine === 'both') {
      this.logger.log('Go 엔진 사이드카 활성화');
    }
    if (this.engine === 'kafka') {
      this.logger.log('Kafka 프로듀서 백엔드 활성화');
    }
  }

  getEngine(): WorkerEngine {
    return this.engine;
  }

  async dispatchToGo(task: QueueTask): Promise<GoEngineResult> {
    return this.goEngineClient.execute(task);
  }

  async dispatchToKafka(task: QueueTask): Promise<WorkerBackendResult> {
    return this.kafkaProducerBackend.execute(task);
  }
}
