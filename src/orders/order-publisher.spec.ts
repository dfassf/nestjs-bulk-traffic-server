import {
  OrderPublisher,
  OrderPublisherConfig,
  orderPublisherConfigFromEnv,
} from './order-publisher';
import { OrderEventPayload, OrderEventType } from './order-events';

jest.mock('kafkajs', () => {
  const sendMock = jest.fn();
  const connectMock = jest.fn();
  const disconnectMock = jest.fn();
  const producerCtor = jest.fn(() => ({
    connect: connectMock,
    disconnect: disconnectMock,
    send: sendMock,
  }));
  const kafkaCtor = jest.fn(() => ({ producer: producerCtor }));
  return {
    Kafka: kafkaCtor,
    logLevel: { WARN: 2 },
    __mocks: { sendMock, connectMock, disconnectMock, producerCtor, kafkaCtor },
  };
});

const mocks = () => (jest.requireMock('kafkajs') as any).__mocks;

function buildConfig(
  overrides: Partial<OrderPublisherConfig> = {},
): OrderPublisherConfig {
  return {
    brokers: ['localhost:9092'],
    clientId: 'test-publisher',
    enabled: true,
    idempotent: true,
    acks: -1,
    disableKey: false,
    ...overrides,
  };
}

function buildPayload(
  overrides: Partial<OrderEventPayload> = {},
): OrderEventPayload {
  return {
    eventType: OrderEventType.CREATED,
    orderId: 'ord-1',
    userId: 'user-1',
    amount: 10000,
    items: [{ productId: 'prod-A', quantity: 2, unitPrice: 5000 }],
    emittedAt: 1_700_000_000_000,
    ...overrides,
  };
}

function stubSend(topic = 'orders.created', partition = 0, offset = '5') {
  mocks().sendMock.mockResolvedValue([
    { topicName: topic, partition, baseOffset: offset, errorCode: 0 },
  ]);
}

describe('orderPublisherConfigFromEnv', () => {
  const envKeys = [
    'ORDER_PRODUCER_IDEMPOTENT',
    'ORDER_PRODUCER_CLIENT_ID',
    'ORDER_PRODUCER_ACKS',
    'ORDER_PRODUCER_DISABLE_KEY',
    'KAFKA_BROKERS',
  ];

  afterEach(() => envKeys.forEach((k) => delete process.env[k]));

  it('기본값은 멱등성 켜짐, acks 전체 확인, 키 사용', () => {
    const config = orderPublisherConfigFromEnv(true);
    expect(config.idempotent).toBe(true);
    expect(config.acks).toBe(-1);
    expect(config.disableKey).toBe(false);
  });

  it('멱등성을 끌 수 있다 (중복 실험용)', () => {
    process.env.ORDER_PRODUCER_IDEMPOTENT = 'false';
    expect(orderPublisherConfigFromEnv(true).idempotent).toBe(false);
  });

  // 멱등 프로듀서는 acks=-1 이 전제다. 다른 값을 주면 kafkajs 가 거부한다.
  it('멱등성이 켜져 있으면 acks 환경변수를 무시하고 -1 을 쓴다', () => {
    process.env.ORDER_PRODUCER_ACKS = '1';
    expect(orderPublisherConfigFromEnv(true).acks).toBe(-1);
  });

  it('멱등성을 끄면 acks 를 조절할 수 있다', () => {
    process.env.ORDER_PRODUCER_IDEMPOTENT = 'false';
    process.env.ORDER_PRODUCER_ACKS = '1';
    expect(orderPublisherConfigFromEnv(true).acks).toBe(1);
  });

  it('키 사용을 끌 수 있다 (순서 붕괴 실험용)', () => {
    process.env.ORDER_PRODUCER_DISABLE_KEY = 'true';
    expect(orderPublisherConfigFromEnv(true).disableKey).toBe(true);
  });
});

describe('OrderPublisher', () => {
  beforeEach(() => jest.clearAllMocks());

  describe('연결', () => {
    it('enabled=false 면 카프카에 연결하지 않는다', async () => {
      const publisher = new OrderPublisher(buildConfig({ enabled: false }));
      await publisher.onModuleInit();

      expect(mocks().kafkaCtor).not.toHaveBeenCalled();
      expect(publisher.isConnected()).toBe(false);
    });

    it('멱등성 설정을 프로듀서 생성에 넘긴다', async () => {
      const publisher = new OrderPublisher(buildConfig({ idempotent: false }));
      await publisher.onModuleInit();

      expect(mocks().producerCtor).toHaveBeenCalledWith({ idempotent: false });
      expect(publisher.isConnected()).toBe(true);
    });

    it('브로커 주소가 비면 연결 대신 예외를 던진다', async () => {
      const publisher = new OrderPublisher(buildConfig({ brokers: [] }));
      await expect(publisher.onModuleInit()).rejects.toThrow(
        /브로커 주소가 비어/,
      );
    });

    it('연결 전에 발행하면 예외를 던진다', async () => {
      const publisher = new OrderPublisher(buildConfig({ enabled: false }));
      await publisher.onModuleInit();

      await expect(publisher.publish(buildPayload())).rejects.toThrow(
        /연결되지 않았습니다/,
      );
    });

    it('종료 시 프로듀서를 끊는다', async () => {
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();
      await publisher.onModuleDestroy();

      expect(mocks().disconnectMock).toHaveBeenCalled();
      expect(publisher.isConnected()).toBe(false);
    });
  });

  describe('발행', () => {
    it('이벤트 종류에 맞는 토픽으로 보낸다', async () => {
      stubSend('payments.approved', 1, '7');
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();

      const result = await publisher.publish(
        buildPayload({ eventType: OrderEventType.PAYMENT_APPROVED }),
      );

      expect(mocks().sendMock).toHaveBeenCalledWith(
        expect.objectContaining({ topic: 'payments.approved' }),
      );
      expect(result.topic).toBe('payments.approved');
      expect(result.partition).toBe(1);
      expect(result.offset).toBe('7');
    });

    it('주문 이벤트는 orderId 를 키로 보낸다', async () => {
      stubSend();
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();

      const result = await publisher.publish(
        buildPayload({ orderId: 'ord-99' }),
      );

      expect(mocks().sendMock.mock.calls[0][0].messages[0].key).toBe('ord-99');
      expect(result.key).toBe('ord-99');
    });

    it('재고 이벤트는 productId 를 키로 보낸다', async () => {
      stubSend('inventory.reserved');
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();

      await publisher.publish(
        buildPayload({
          eventType: OrderEventType.INVENTORY_RESERVED,
          items: [{ productId: 'prod-Z', quantity: 1, unitPrice: 100 }],
        }),
      );

      expect(mocks().sendMock.mock.calls[0][0].messages[0].key).toBe('prod-Z');
    });

    // 키를 빼면 파티션이 흩어져 순서가 깨진다. 실험용 스위치가 실제로 먹는지 본다.
    it('disableKey 면 키 없이 보낸다', async () => {
      stubSend();
      const publisher = new OrderPublisher(buildConfig({ disableKey: true }));
      await publisher.onModuleInit();

      const result = await publisher.publish(buildPayload());

      expect(mocks().sendMock.mock.calls[0][0].messages[0].key).toBeNull();
      expect(result.key).toBeNull();
    });

    it('acks 설정을 발행에 반영한다', async () => {
      stubSend();
      const publisher = new OrderPublisher(
        buildConfig({ idempotent: false, acks: 1 }),
      );
      await publisher.onModuleInit();

      await publisher.publish(buildPayload());

      expect(mocks().sendMock).toHaveBeenCalledWith(
        expect.objectContaining({ acks: 1 }),
      );
    });

    it('본문에 이벤트 종류와 주문 정보를 담는다', async () => {
      stubSend();
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();

      await publisher.publish(buildPayload({ amount: 33000 }));

      const value = JSON.parse(
        mocks().sendMock.mock.calls[0][0].messages[0].value,
      );
      expect(value.eventType).toBe(OrderEventType.CREATED);
      expect(value.orderId).toBe('ord-1');
      expect(value.amount).toBe(33000);
      expect(value.items).toHaveLength(1);
    });

    it('여러 건을 순서대로 발행한다', async () => {
      stubSend();
      const publisher = new OrderPublisher(buildConfig());
      await publisher.onModuleInit();

      const results = await publisher.publishMany([
        buildPayload({ orderId: 'ord-1' }),
        buildPayload({ orderId: 'ord-2' }),
      ]);

      expect(results).toHaveLength(2);
      expect(mocks().sendMock).toHaveBeenCalledTimes(2);
      expect(mocks().sendMock.mock.calls[0][0].messages[0].key).toBe('ord-1');
      expect(mocks().sendMock.mock.calls[1][0].messages[0].key).toBe('ord-2');
    });
  });
});
