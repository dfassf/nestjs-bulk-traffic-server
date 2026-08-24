import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { OrderEventPayload, pickEventKey, topicFor } from './order-events';
import { readKafkaBrokersEnv } from '../queue/utils/env';

export const ORDER_PUBLISHER_CONFIG = Symbol('ORDER_PUBLISHER_CONFIG');

export interface OrderPublisherConfig {
  brokers: string[];
  clientId: string;
  /** false 면 연결하지 않는다. 카프카 없이 서버만 띄울 때. */
  enabled: boolean;
  /**
   * 프로듀서 재시도로 생기는 브로커 중복을 막을지.
   *
   * 실험 대상이라 끌 수 있게 뒀다. 끄고 브로커를 일시 차단하면
   * 같은 메시지가 두 번 저장되는 것을 볼 수 있다.
   */
  idempotent: boolean;
  /**
   * 몇 개의 복제본이 받아야 성공으로 볼지.
   *   -1 = 동기화된 복제본 전부(가장 안전)
   *    1 = 리더만
   *    0 = 확인 안 함(가장 빠르고 위험)
   * kafkajs 는 idempotent 를 켜면 -1 을 강제한다.
   */
  acks: number;
  /** 키 없이 발행할지. 순서가 깨지는 것을 보려고 켠다. */
  disableKey: boolean;
}

export function orderPublisherConfigFromEnv(
  enabled: boolean,
): OrderPublisherConfig {
  const idempotent = process.env.ORDER_PRODUCER_IDEMPOTENT !== 'false';
  return {
    brokers: readKafkaBrokersEnv(),
    clientId: process.env.ORDER_PRODUCER_CLIENT_ID ?? 'order-publisher',
    enabled,
    idempotent,
    // 멱등 프로듀서는 acks=-1 이 전제다. 다른 값을 주면 kafkajs 가 거부한다.
    acks: idempotent ? -1 : Number(process.env.ORDER_PRODUCER_ACKS ?? -1),
    disableKey: process.env.ORDER_PRODUCER_DISABLE_KEY === 'true',
  };
}

export interface PublishResult {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
}

/**
 * 주문 이벤트를 카프카로 내보낸다.
 *
 * 큐 작업(QueueTask)을 보내는 KafkaProducerBackend 와 목적이 달라 따로 둔다.
 * 이쪽은 도메인 이벤트 전용이고, 실험을 위해 멱등성·acks·키 사용 여부를
 * 설정으로 바꿀 수 있게 열어뒀다.
 */
@Injectable()
export class OrderPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OrderPublisher.name);
  private readonly config: OrderPublisherConfig;

  private kafka: Kafka;
  private producer: Producer;
  private connected = false;

  constructor(
    @Optional()
    @Inject(ORDER_PUBLISHER_CONFIG)
    config?: OrderPublisherConfig,
  ) {
    this.config = config ?? orderPublisherConfigFromEnv(true);
  }

  async onModuleInit(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.log(
        '주문 이벤트 발행이 꺼져 있어 카프카에 연결하지 않습니다.',
      );
      return;
    }
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.connected && this.producer) {
      await this.producer.disconnect();
      this.connected = false;
      this.logger.log('주문 이벤트 프로듀서 연결 해제');
    }
  }

  private async connect(): Promise<void> {
    const { brokers, clientId, idempotent } = this.config;
    if (brokers.length === 0) {
      throw new Error(
        'Kafka 브로커 주소가 비어 있습니다. KAFKA_BROKERS 를 확인하세요.',
      );
    }

    this.kafka = new Kafka({ clientId, brokers, logLevel: logLevel.WARN });
    this.producer = this.kafka.producer({ idempotent });
    await this.producer.connect();
    this.connected = true;

    this.logger.log(
      `주문 이벤트 프로듀서 연결: brokers=[${brokers.join(',')}] ` +
        `idempotent=${idempotent} acks=${this.config.acks} ` +
        `키사용=${!this.config.disableKey}`,
    );
  }

  isConnected(): boolean {
    return this.connected;
  }

  getConfig(): Readonly<OrderPublisherConfig> {
    return this.config;
  }

  async publish(payload: OrderEventPayload): Promise<PublishResult> {
    if (!this.connected) {
      throw new Error(
        '주문 이벤트 프로듀서가 연결되지 않았습니다. 카프카 기동 상태와 설정을 확인하세요.',
      );
    }

    const topic = topicFor(payload.eventType);
    // 키를 빼면 파티션이 흩어져 한 주문의 이벤트 순서가 깨진다. 그걸 보려는 실험용 스위치다.
    const key = this.config.disableKey ? null : pickEventKey(payload);

    const [record] = await this.producer.send({
      topic,
      acks: this.config.acks,
      messages: [{ key, value: JSON.stringify(payload) }],
    });

    return {
      topic: record.topicName,
      partition: record.partition,
      offset: String(record.baseOffset),
      key,
    };
  }

  /** 여러 이벤트를 한 번에 보낸다. 대량 발행 실험용. */
  async publishMany(payloads: OrderEventPayload[]): Promise<PublishResult[]> {
    const results: PublishResult[] = [];
    for (const payload of payloads) {
      results.push(await this.publish(payload));
    }
    return results;
  }
}
