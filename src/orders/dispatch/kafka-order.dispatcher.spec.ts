import { KafkaOrderDispatcher } from './kafka-order.dispatcher';
import { OrderPublisher } from '../order-publisher';
import { OrderEventPayload, OrderEventType } from '../order-events';

describe('KafkaOrderDispatcher', () => {
  let publisher: jest.Mocked<OrderPublisher>;
  let dispatcher: KafkaOrderDispatcher;

  beforeEach(() => {
    publisher = {
      publish: jest.fn().mockResolvedValue({
        topic: 'orders.created',
        partition: 3,
        offset: '42',
        key: 'ord-1',
      }),
      isConnected: jest.fn().mockReturnValue(true),
    } as unknown as jest.Mocked<OrderPublisher>;

    dispatcher = new KafkaOrderDispatcher(publisher);
  });

  function buildEvent(): OrderEventPayload {
    return {
      eventType: OrderEventType.CREATED,
      orderId: 'ord-1',
      userId: 'user-1',
      amount: 10000,
      items: [{ productId: 'p', quantity: 1, unitPrice: 10000 }],
      emittedAt: Date.now(),
    };
  }

  it('어느 수단인지 이름으로 드러낸다', () => {
    expect(dispatcher.name).toBe('kafka');
  });

  it('발행 결과를 통로 형태로 옮긴다', async () => {
    const result = await dispatcher.dispatch(buildEvent());

    expect(result).toEqual({
      destination: 'orders.created',
      partition: 3,
      offset: '42',
      key: 'ord-1',
    });
  });

  it('연결 상태를 발행자에게 묻는다', () => {
    (publisher.isConnected as jest.Mock).mockReturnValue(false);

    expect(dispatcher.isReady()).toBe(false);
  });

  // 발행 실패를 삼키면 주문은 저장됐는데 이벤트만 없는 상태가 조용히 생긴다.
  it('발행 실패를 그대로 올려보낸다', async () => {
    publisher.publish.mockRejectedValue(new Error('브로커 연결 끊김'));

    await expect(dispatcher.dispatch(buildEvent())).rejects.toThrow(
      /브로커 연결 끊김/,
    );
  });
});
