import { OrderService } from './order.service';
import { OrderPublisher } from './order-publisher';
import { OrderStore } from './order-store.interface';
import { Order, OrderEventType, OrderStatus } from './order-events';

describe('OrderService', () => {
  let store: jest.Mocked<OrderStore>;
  let publisher: jest.Mocked<OrderPublisher>;
  let service: OrderService;

  const savedOrders: Order[] = [];

  beforeEach(() => {
    savedOrders.length = 0;

    store = {
      init: jest.fn(),
      destroy: jest.fn(),
      saveOrder: jest.fn(async (order: Order) => {
        savedOrders.push(order);
      }),
      findOrder: jest.fn(),
      updateStatus: jest.fn(),
      recordEvent: jest.fn(),
      findEvents: jest.fn().mockResolvedValue([]),
      findDuplicates: jest.fn().mockResolvedValue([]),
      countOrders: jest.fn().mockResolvedValue(0),
      countEvents: jest.fn().mockResolvedValue(0),
      reset: jest.fn(),
    } as unknown as jest.Mocked<OrderStore>;

    publisher = {
      publish: jest.fn().mockResolvedValue({
        topic: 'orders.created',
        partition: 0,
        offset: '1',
        key: 'ord-1',
      }),
      isConnected: jest.fn().mockReturnValue(true),
      getConfig: jest.fn().mockReturnValue({
        brokers: ['localhost:9092'],
        clientId: 'test',
        enabled: true,
        idempotent: true,
        acks: -1,
        disableKey: false,
      }),
    } as unknown as jest.Mocked<OrderPublisher>;

    service = new OrderService(store, publisher);
  });

  describe('주문 생성', () => {
    it('주문을 저장하고 생성 이벤트를 발행한다', async () => {
      const result = await service.createOrder({ userId: 'user-9' });

      expect(store.saveOrder).toHaveBeenCalledTimes(1);
      expect(publisher.publish).toHaveBeenCalledTimes(1);
      expect(result.order.userId).toBe('user-9');
      expect(result.order.status).toBe(OrderStatus.CREATED);
      expect(result.dispatch.topic).toBe('orders.created');
    });

    it('품목 금액을 합쳐 주문 금액을 낸다', async () => {
      const result = await service.createOrder({
        items: [
          { productId: 'a', quantity: 2, unitPrice: 5000 },
          { productId: 'b', quantity: 3, unitPrice: 1000 },
        ],
      });

      expect(result.order.amount).toBe(13000);
    });

    it('품목을 안 주면 기본 품목 한 건으로 만든다', async () => {
      const result = await service.createOrder({});

      expect(result.order.items).toHaveLength(1);
      expect(result.order.amount).toBeGreaterThan(0);
    });

    it('발행 본문에 생성 이벤트 종류를 담는다', async () => {
      await service.createOrder({});

      const payload = publisher.publish.mock.calls[0][0];
      expect(payload.eventType).toBe(OrderEventType.CREATED);
    });

    // 발행 실패를 삼키면 주문은 있는데 이벤트가 없는 상태를 아무도 모른다.
    it('발행이 실패하면 예외를 그대로 올린다', async () => {
      publisher.publish.mockRejectedValue(new Error('브로커 연결 끊김'));

      await expect(service.createOrder({})).rejects.toThrow(/브로커 연결 끊김/);
    });

    it('주문마다 다른 식별자를 준다', async () => {
      await service.createOrder({});
      await service.createOrder({});

      expect(savedOrders[0].orderId).not.toBe(savedOrders[1].orderId);
    });
  });

  describe('대량 생성', () => {
    it('요청한 건수만큼 만들고 파티션 분포를 돌려준다', async () => {
      publisher.publish
        .mockResolvedValueOnce({
          topic: 't',
          partition: 0,
          offset: '1',
          key: 'k',
        })
        .mockResolvedValueOnce({
          topic: 't',
          partition: 1,
          offset: '2',
          key: 'k',
        })
        .mockResolvedValueOnce({
          topic: 't',
          partition: 0,
          offset: '3',
          key: 'k',
        });

      const result = await service.createBulk(3);

      expect(result.requested).toBe(3);
      expect(result.created).toBe(3);
      expect(result.failed).toBe(0);
      expect(result.partitionCounts).toEqual({ 0: 2, 1: 1 });
    });

    // 개별 실패를 관용하되 건수는 세서 보고해야 한다.
    // 조용히 넘기면 100건 중 40건이 실패해도 "완료" 로 보인다.
    it('일부 실패해도 계속 진행하고 실패 건수를 보고한다', async () => {
      publisher.publish
        .mockResolvedValueOnce({
          topic: 't',
          partition: 0,
          offset: '1',
          key: 'k',
        })
        .mockRejectedValueOnce(new Error('일시 장애'))
        .mockResolvedValueOnce({
          topic: 't',
          partition: 0,
          offset: '2',
          key: 'k',
        });

      const result = await service.createBulk(3);

      expect(result.created).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors).toContain('일시 장애');
    });

    it('전량 실패해도 건수로 드러난다', async () => {
      publisher.publish.mockRejectedValue(new Error('브로커 다운'));

      const result = await service.createBulk(5);

      expect(result.created).toBe(0);
      expect(result.failed).toBe(5);
      expect(result.partitionCounts).toEqual({});
    });

    it('오류 목록은 앞의 몇 건만 남긴다', async () => {
      publisher.publish.mockRejectedValue(new Error('실패'));

      const result = await service.createBulk(20);

      expect(result.failed).toBe(20);
      expect(result.errors.length).toBeLessThanOrEqual(5);
    });
  });

  describe('통계', () => {
    it('중복 초과분을 세서 알린다', async () => {
      store.countOrders.mockResolvedValue(10);
      store.countEvents.mockResolvedValue(13);
      store.findDuplicates.mockResolvedValue([
        { orderId: 'ord-1', eventType: OrderEventType.CREATED, count: 3 },
        { orderId: 'ord-2', eventType: OrderEventType.CREATED, count: 2 },
      ]);

      const stats = await service.getStats();

      expect(stats.orderCount).toBe(10);
      expect(stats.eventCount).toBe(13);
      expect(stats.duplicateGroups).toBe(2);
      // 3건 중 2건 초과 + 2건 중 1건 초과 = 3
      expect(stats.duplicateExtra).toBe(3);
    });

    it('프로듀서 설정을 함께 보고한다', async () => {
      const stats = await service.getStats();

      expect(stats.producer.connected).toBe(true);
      expect(stats.producer.idempotent).toBe(true);
      expect(stats.producer.keyEnabled).toBe(true);
    });
  });
});
