import { Module } from '@nestjs/common';
import { OrderController } from './order.controller';
import { OrderService } from './order.service';
import {
  OrderPublisher,
  ORDER_PUBLISHER_CONFIG,
  orderPublisherConfigFromEnv,
} from './order-publisher';
import { ORDER_STORE } from './order-store.interface';
import { SqliteOrderStore } from './sqlite-order.store';
import {
  OrderTopicProvisioner,
  TOPIC_PROVISIONER_CONFIG,
  topicProvisionerConfigFromEnv,
} from './order-topic.provisioner';
import { LabController } from './lab/lab.controller';
import { ConsumerProcessManager } from './lab/consumer-process.manager';
import {
  KafkaInspector,
  KAFKA_INSPECTOR_CONFIG,
  inspectorConfigFromEnv,
} from './lab/kafka-inspector';
import { readWorkerEngineEnv } from '../queue/utils/env';

/**
 * 카프카 실험용 주문 도메인.
 *
 * 실제 이커머스 서비스가 아니라, 카프카가 어떤 상황에서 어떻게 동작하는지
 * 관찰할 재료다. 배경은 docs/kafka-lab-plan.md 참고.
 */
@Module({
  controllers: [OrderController, LabController],
  providers: [
    OrderService,
    OrderPublisher,
    OrderTopicProvisioner,
    ConsumerProcessManager,
    KafkaInspector,
    {
      provide: KAFKA_INSPECTOR_CONFIG,
      useFactory: () => inspectorConfigFromEnv(),
    },
    {
      // 발행자가 스스로 켜짐 여부를 판단하지 않도록 모듈이 정해서 넘긴다.
      // QueueModule 의 KAFKA_PRODUCER_CONFIG 와 같은 방식이다.
      provide: ORDER_PUBLISHER_CONFIG,
      useFactory: () =>
        orderPublisherConfigFromEnv(readWorkerEngineEnv() === 'kafka'),
    },
    {
      provide: TOPIC_PROVISIONER_CONFIG,
      useFactory: () =>
        topicProvisionerConfigFromEnv(readWorkerEngineEnv() === 'kafka'),
    },
    {
      provide: ORDER_STORE,
      useFactory: async () => {
        const store = new SqliteOrderStore();
        await store.init();
        return store;
      },
    },
  ],
  exports: [OrderService],
})
export class OrderModule {}
