import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { GoEngineClient, GoEngineResult } from './go-engine.client';
import { GoEngineBackend } from './go-engine.backend';
import { KafkaProducerBackend } from './kafka-producer.backend';
import { WorkerBackend, WorkerBackendResult } from './interfaces/worker-backend.interface';
import { readWorkerEngineEnv, WorkerEngine } from './utils/env';

export { WorkerEngine };

@Injectable()
export class EngineRouterService implements OnModuleInit {
  private readonly logger = new Logger(EngineRouterService.name);
  private readonly engine: WorkerEngine;

  constructor(
    private readonly goEngineClient: GoEngineClient,
    private readonly goEngineBackend: GoEngineBackend,
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

  /**
   * 현재 엔진 모드에 해당하는 백엔드. node·both 모드는 메인 스레드·워커풀이
   * 담당하므로 여기서는 null 을 돌려준다(호출 측이 기존 경로를 그대로 사용).
   */
  getBackend(): WorkerBackend | null {
    if (this.engine === 'kafka') return this.kafkaProducerBackend;
    if (this.engine === 'go') return this.goEngineBackend;
    return null;
  }

  /** 엔진 모드와 무관하게 Go 엔진으로 직접 보낸다(엔진 비교 벤치마크 전용). */
  async dispatchToGo(task: QueueTask): Promise<GoEngineResult> {
    return this.goEngineClient.execute(task);
  }

  async dispatchToKafka(task: QueueTask): Promise<WorkerBackendResult> {
    return this.kafkaProducerBackend.execute(task);
  }
}
