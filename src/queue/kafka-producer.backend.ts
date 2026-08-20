import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { QueueTask } from './interfaces/queue-task.interface';
import { WorkerBackend, WorkerBackendResult } from './interfaces/worker-backend.interface';
import { readKafkaBrokersEnv, readWorkerEngineEnv } from './utils/env';

const TOPIC_HIGH = 'tasks.high';
const TOPIC_NORMAL = 'tasks.normal';
const TOPIC_LOW = 'tasks.low';

/**
 * Kafka 프로듀서 기반 워커 백엔드.
 *
 * QueueTask를 우선순위별 토픽으로 발행한다.
 *   priority >= 5  -> tasks.high
 *   priority >= 0  -> tasks.normal
 *   priority < 0   -> tasks.low
 *
 * fire-and-forget 방식이라 execute()는 발행 접수 응답만 리턴한다.
 * 실제 처리는 별도 Go 컨슈머에서 수행하며 결과 토픽으로 회신한다.
 */
@Injectable()
export class KafkaProducerBackend implements WorkerBackend, OnModuleInit, OnModuleDestroy {
  readonly name = 'kafka';
  private readonly logger = new Logger(KafkaProducerBackend.name);

  private kafka: Kafka;
  private producer: Producer;
  private connected = false;

  async onModuleInit(): Promise<void> {
    // 엔진 판정은 utils/env 의 readWorkerEngineEnv 한 곳에서만 한다.
    // 여기서 문자열을 직접 비교하면 EngineRouterService 와 해석이 갈린다.
    const engine = readWorkerEngineEnv();
    if (engine !== 'kafka') {
      this.logger.log(`워커 엔진이 ${engine} 이라 Kafka 프로듀서 초기화를 건너뜁니다.`);
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
    const brokers = readKafkaBrokersEnv();
    const clientId = process.env.KAFKA_CLIENT_ID ?? 'bulk-traffic-producer';

    this.kafka = new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.WARN,
    });

    this.producer = this.kafka.producer();
    await this.producer.connect();
    this.connected = true;
    this.logger.log(`Kafka 프로듀서 연결 성공: brokers=[${brokers.join(',')}] clientId=${clientId}`);
  }

  isConnected(): boolean {
    return this.connected;
  }

  async execute(task: QueueTask): Promise<WorkerBackendResult> {
    if (!this.connected) {
      throw new Error('Kafka 프로듀서가 연결되지 않았습니다. WORKER_ENGINE=kafka 여부와 브로커 상태를 확인하세요.');
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
