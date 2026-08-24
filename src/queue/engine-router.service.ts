import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';
import { GoEngineClient, GoEngineResult } from './go-engine.client';
import { GoEngineBackend } from './go-engine.backend';
import { KafkaProducerBackend } from './kafka-producer.backend';
import {
  WorkerBackend,
  WorkerBackendResult,
} from './interfaces/worker-backend.interface';
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
      // 프로듀서는 연결되지만 큐 처리 본류는 아직 이 백엔드를 거치지 않는다.
      // 여기서 '활성화'라고만 적으면 작업이 카프카로 나가는 줄로 읽힌다.
      this.logger.warn(
        'Kafka 프로듀서는 연결되지만 작업 발행 경로는 아직 연결되지 않았습니다. ' +
          '이 모드에서도 큐 처리는 Node 워커풀이 담당합니다.',
      );
    }
  }

  getEngine(): WorkerEngine {
    return this.engine;
  }

  /** 큐 처리 본류가 이 백엔드를 거치는지. 지금은 어느 모드에서도 거치지 않는다. */
  isBackendWired(): boolean {
    return false;
  }

  /**
   * 현재 엔진 모드에 해당하는 백엔드.
   *
   * 아직 큐 처리 본류(QueueProcessorService)에 연결되어 있지 않다.
   * 연결 작업은 docs/kafka-integration-plan.md 의 다음 단계다.
   * node·both 모드는 메인 스레드·워커풀이 담당하므로 null 을 돌려준다.
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
