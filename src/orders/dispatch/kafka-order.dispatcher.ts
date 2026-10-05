import { Injectable } from '@nestjs/common';
import { OrderEventPayload } from '../order-events';
import { OrderPublisher } from '../order-publisher';
import { DispatchResult, OrderDispatcher } from './order-dispatcher.interface';

/**
 * 카프카로 내보내는 통로.
 *
 * 기존 OrderPublisher 를 그대로 쓴다. 발행 로직을 여기로 옮기지 않는다.
 * 옮기면 카프카 실험들이 쓰는 설정(멱등성·acks·키 사용 여부)이 두 군데로 갈린다.
 */
@Injectable()
export class KafkaOrderDispatcher implements OrderDispatcher {
  readonly name = 'kafka';

  constructor(private readonly publisher: OrderPublisher) {}

  isReady(): boolean {
    return this.publisher.isConnected();
  }

  async dispatch(payload: OrderEventPayload): Promise<DispatchResult> {
    const result = await this.publisher.publish(payload);

    return {
      destination: result.topic,
      partition: result.partition,
      offset: result.offset,
      key: result.key,
    };
  }
}
