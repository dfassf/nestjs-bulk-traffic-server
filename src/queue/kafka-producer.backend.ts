import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { QueueTask } from './interfaces/queue-task.interface';
import {
  WorkerBackend,
  WorkerBackendResult,
} from './interfaces/worker-backend.interface';
import { readKafkaBrokersEnv } from './utils/env';

const TOPIC_HIGH = 'tasks.high';
const TOPIC_NORMAL = 'tasks.normal';
const TOPIC_LOW = 'tasks.low';

export const KAFKA_PRODUCER_CONFIG = Symbol('KAFKA_PRODUCER_CONFIG');

export interface KafkaProducerConfig {
  /** 브로커 주소 목록. 비어 있으면 연결하지 않는다. */
  brokers: string[];
  clientId: string;
  /** false 면 onModuleInit 에서 연결을 건너뛴다(엔진이 kafka 가 아닐 때). */
  enabled: boolean;
}

/** 환경변수에서 프로듀서 설정을 만든다. 모듈 등록부(useFactory)에서 사용. */
export function kafkaProducerConfigFromEnv(
  enabled: boolean,
): KafkaProducerConfig {
  return {
    brokers: readKafkaBrokersEnv(),
    clientId: process.env.KAFKA_CLIENT_ID ?? 'bulk-traffic-producer',
    enabled,
  };
}

/**
 * Kafka 프로듀서 기반 워커 백엔드.
 *
 * QueueTask 를 우선순위별 토픽으로 발행한다.
 *   priority >= 5  -> tasks.high
 *   priority >= 0  -> tasks.normal
 *   priority < 0   -> tasks.low
 *
 * 보내고 끝내는 방식이라 execute() 는 발행 접수 응답만 돌려준다.
 * 실제 처리는 별도 Go 컨슈머가 맡고 결과는 결과 토픽으로 회신한다.
 *
 * 켜짐 여부는 이 클래스가 아니라 QueueModule 이 정한다(config.enabled).
 */
@Injectable()
export class KafkaProducerBackend
  implements WorkerBackend, OnModuleInit, OnModuleDestroy
{
  readonly name = 'kafka';
  private readonly logger = new Logger(KafkaProducerBackend.name);
  private readonly config: KafkaProducerConfig;

  private kafka: Kafka;
  private producer: Producer;
  private connected = false;

  constructor(
    @Optional()
    @Inject(KAFKA_PRODUCER_CONFIG)
    config?: KafkaProducerConfig,
  ) {
    // 주입이 없으면 환경변수로 폴백한다(테스트·직접 생성 편의).
    this.config = config ?? kafkaProducerConfigFromEnv(true);
  }

  async onModuleInit(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.log(
        '워커 엔진이 kafka 가 아니라 프로듀서 초기화를 건너뜁니다.',
      );
      return;
    }
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.connected && this.producer) {
      await this.producer.disconnect();
      this.connected = false;
      this.logger.log('Kafka 프로듀서 연결 해제');
    }
  }

  private async connect(): Promise<void> {
    const { brokers, clientId } = this.config;
    if (brokers.length === 0) {
      throw new Error(
        'Kafka 브로커 주소가 비어 있습니다. KAFKA_BROKERS 를 확인하세요.',
      );
    }

    this.kafka = new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.WARN,
    });

    this.producer = this.kafka.producer();
    await this.producer.connect();
    this.connected = true;
    this.logger.log(
      `Kafka 프로듀서 연결 성공: brokers=[${brokers.join(',')}] clientId=${clientId}`,
    );
  }

  isConnected(): boolean {
    return this.connected;
  }

  async execute(task: QueueTask): Promise<WorkerBackendResult> {
    if (!this.connected) {
      throw new Error(
        'Kafka 프로듀서가 연결되지 않았습니다. WORKER_ENGINE 값과 브로커 상태를 확인하세요.',
      );
    }

    const topic = pickTopic(task.priority);
    const key = String(task.requestId ?? task.id);
    const payload = {
      taskId: task.id,
      requestId: task.requestId,
      priority: task.priority,
      workloadType: task.workloadType,
      category: task.category,
      size: task.size,
      params: task.params ?? {},
      functionCode: task.functionCode,
      timeout: task.timeout,
      enqueuedAt: task.timestamp,
    };

    const result = await this.producer.send({
      topic,
      messages: [
        {
          key,
          value: JSON.stringify(payload),
        },
      ],
    });

    const record = result[0];
    return {
      mode: 'async',
      taskId: String(task.id),
      dispatch: {
        topic: record.topicName,
        partition: record.partition,
        offset: String(record.baseOffset),
      },
      backend: this.name,
    };
  }

  async healthCheck(): Promise<boolean> {
    if (!this.connected) return false;
    try {
      const admin = this.kafka.admin();
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return true;
    } catch (err) {
      this.logger.warn(`Kafka 헬스체크 실패: ${(err as Error).message}`);
      return false;
    }
  }
}

/**
 * priority 값을 우선순위별 토픽으로 매핑.
 * 기존 QueueService 규칙(priority >= 5 high, >= 0 normal, < 0 low)과 동일하게 유지.
 */
export function pickTopic(priority: number): string {
  if (priority >= 5) return TOPIC_HIGH;
  if (priority >= 0) return TOPIC_NORMAL;
  return TOPIC_LOW;
}

export const KAFKA_TOPICS = {
  HIGH: TOPIC_HIGH,
  NORMAL: TOPIC_NORMAL,
  LOW: TOPIC_LOW,
} as const;
