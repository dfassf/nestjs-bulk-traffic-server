import { OrderService } from './order.service';
import { OrderPublisher } from './order-publisher';
import { OrderStore } from './order-store.interface';
import { Order, OrderEventType, OrderStatus } from './order-events';
import { OrderDispatcher } from './dispatch/order-dispatcher.interface';

describe('OrderService', () => {
  let store: jest.Mocked<OrderStore>;
  let publisher: jest.Mocked<OrderPublisher>;
  let dispatcher: jest.Mocked<OrderDispatcher>;
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

    // 내보내는 일은 통로가 맡는다. 서비스는 무슨 수단인지 모른다.
    dispatcher = {
      name: 'kafka',
      isReady: jest.fn().mockReturnValue(true),
      dispatch: jest.fn().mockResolvedValue({
        destination: 'orders.created',
        partition: 0,
        offset: '1',
        key: 'ord-1',
      }),
    } as unknown as jest.Mocked<OrderDispatcher>;

    service = new OrderService(store, dispatcher, publisher);
  });

  describe('주문 생성', () => {
    it('주문을 저장하고 생성 이벤트를 발행한다', async () => {
      const result = await service.createOrder({ userId: 'user-9' });

      expect(store.saveOrder).toHaveBeenCalledTimes(1);
      expect(dispatcher.dispatch).toHaveBeenCalledTimes(1);
      expect(result.order.userId).toBe('user-9');
      expect(result.order.status).toBe(OrderStatus.CREATED);
      expect(result.dispatch.destination).toBe('orders.created');
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

      const payload = dispatcher.dispatch.mock.calls[0][0];
      expect(payload.eventType).toBe(OrderEventType.CREATED);
    });

    // 발행 실패를 삼키면 주문은 있는데 이벤트가 없는 상태를 아무도 모른다.
    it('발행이 실패하면 예외를 그대로 올린다', async () => {
      dispatcher.dispatch.mockRejectedValue(new Error('브로커 연결 끊김'));

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
      dispatcher.dispatch
        .mockResolvedValueOnce({
          destination: 't',
          partition: 0,
          offset: '1',
          key: 'k',
        })
        .mockResolvedValueOnce({
          destination: 't',
          partition: 1,
          offset: '2',
          key: 'k',
        })
        .mockResolvedValueOnce({
          destination: 't',
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
      dispatcher.dispatch
        .mockResolvedValueOnce({
          destination: 't',
          partition: 0,
          offset: '1',
          key: 'k',
        })
        .mockRejectedValueOnce(new Error('일시 장애'))
        .mockResolvedValueOnce({
          destination: 't',
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
      dispatcher.dispatch.mockRejectedValue(new Error('브로커 다운'));

      const result = await service.createBulk(5);

      expect(result.created).toBe(0);
      expect(result.failed).toBe(5);
      expect(result.partitionCounts).toEqual({});
    });

    it('오류 목록은 앞의 몇 건만 남긴다', async () => {
      dispatcher.dispatch.mockRejectedValue(new Error('실패'));

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
  describe('주문 흐름 발행', () => {
    beforeEach(() => {
      // 흐름 발행은 저장된 주문을 다시 읽어서 단계를 보낸다.
      store.findOrder.mockImplementation(async (orderId: string) => {
        return savedOrders.find((o) => o.orderId === orderId) ?? null;
      });
    });

    // 보내는 쪽에서 단계 순서를 지켜야 한다. 받는 쪽에서 그 순서가
    // 유지되는지는 키 설정에 달려 있고, 그게 실험 대상이다.
    it('생성 뒤 세 단계를 순서대로 보낸다', async () => {
      const { order } = await service.createOrder({});
      dispatcher.dispatch.mockClear();

      const result = await service.dispatchLifecycle(order.orderId);

      expect(result.steps.map((s) => s.eventType)).toEqual([
        OrderEventType.INVENTORY_RESERVED,
        OrderEventType.PAYMENT_APPROVED,
        OrderEventType.SHIPMENT_STARTED,
      ]);
    });

    // CREATED 를 다시 보내면 생성 이벤트가 두 번 나가 중복 집계가 틀어진다.
    it('생성 이벤트를 다시 보내지 않는다', async () => {
      const { order } = await service.createOrder({});
      dispatcher.dispatch.mockClear();

      await service.dispatchLifecycle(order.orderId);

      const sent = dispatcher.dispatch.mock.calls.map((c) => c[0].eventType);
      expect(sent).not.toContain(OrderEventType.CREATED);
      expect(sent).toHaveLength(3);
    });

    it('단계마다 주문 상태를 올린다', async () => {
      const { order } = await service.createOrder({});

      await service.dispatchLifecycle(order.orderId);

      expect(store.updateStatus.mock.calls.map((c) => c[1])).toEqual([
        OrderStatus.INVENTORY_RESERVED,
        OrderStatus.PAYMENT_APPROVED,
        OrderStatus.SHIPPED,
      ]);
    });

    it('없는 주문이면 에러를 낸다', async () => {
      await expect(service.dispatchLifecycle('ord-없음')).rejects.toThrow(
        /주문을 찾을 수 없습니다/,
      );
    });

    it('대량 발행에서 건수를 세어 보고한다', async () => {
      const result = await service.createWithLifecycle(3);

      expect(result.requested).toBe(3);
      expect(result.completed).toBe(3);
      expect(result.failed).toBe(0);
      expect(result.orderIds).toHaveLength(3);
    });

    // 조용히 넘기면 중간에 끊긴 주문이 있어도 "완료" 로 보인다.
    it('일부가 실패하면 실패 건수로 드러낸다', async () => {
      let call = 0;
      dispatcher.dispatch.mockImplementation(async () => {
        call += 1;
        // 두 번째 주문의 흐름 발행에서 터뜨린다.
        if (call === 6) throw new Error('발행 실패');
        return {
          destination: 'orders.created',
          partition: 0,
          offset: String(call),
          key: 'k',
        };
      });

      const result = await service.createWithLifecycle(3);

      expect(result.failed).toBeGreaterThan(0);
      expect(result.completed + result.failed).toBe(3);
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });
  describe('파티션 흩어짐 집계', () => {
    beforeEach(() => {
      store.findOrder.mockImplementation(async (orderId: string) => {
        return savedOrders.find((o) => o.orderId === orderId) ?? null;
      });
    });

    // 키를 쓰면 같은 주문의 단계가 한 파티션에 모인다.
    it('한 파티션에 모이면 모인 것으로 센다', async () => {
      dispatcher.dispatch.mockResolvedValue({
        destination: 'orders.created',
        partition: 2,
        offset: '1',
        key: 'ord-1',
      });

      const result = await service.createWithLifecycle(2);

      expect(result.scatter.singlePartition).toBe(2);
      expect(result.scatter.multiPartition).toBe(0);
      expect(result.scatter.avgPartitionsPerOrder).toBe(1);
    });

    // 키를 빼면 파티션이 흩어져 받는 쪽 순서가 뒤바뀔 수 있다.
    it('여러 파티션으로 갈라지면 흩어진 것으로 센다', async () => {
      let n = 0;
      dispatcher.dispatch.mockImplementation(async () => ({
        destination: 'orders.created',
        partition: n++ % 6,
        offset: String(n),
        key: null,
      }));

      const result = await service.createWithLifecycle(2);

      expect(result.scatter.multiPartition).toBe(2);
      expect(result.scatter.singlePartition).toBe(0);
      expect(result.scatter.avgPartitionsPerOrder).toBeGreaterThan(1);
    });

    /**
     * 파티션이라는 개념이 없는 통로(프로세스 안의 큐)는 null 을 준다.
     * 0 으로 치면 "0번 파티션에 모였다" 로 읽혀 결과가 뒤집힌다.
     */
    it('파티션을 모르는 통로는 셈에서 빼고 평균을 null 로 둔다', async () => {
      dispatcher.dispatch.mockResolvedValue({
        destination: 'memory://orders',
        partition: null,
        offset: null,
        key: null,
      });

      const result = await service.createWithLifecycle(2);

      expect(result.scatter.singlePartition).toBe(0);
      expect(result.scatter.multiPartition).toBe(0);
      expect(result.scatter.avgPartitionsPerOrder).toBeNull();
    });

    it('눈으로 볼 샘플을 함께 돌려준다', async () => {
      const result = await service.createWithLifecycle(5);

      expect(result.samples).toHaveLength(3);
      expect(result.samples[0].steps).toHaveLength(3);
    });
  });
  describe('대량 생성의 파티션 집계', () => {
    /**
     * 파티션이라는 개념이 없는 통로(프로세스 안의 큐)는 null 을 준다.
     * 예전에는 그 null 이 그대로 키가 되어 응답에 {"null": 10000} 으로 나왔다.
     * 0 으로 바꿔도 안 된다. "0번 파티션" 과 섞여 분포가 틀어진다.
     */
    it('파티션을 모르는 통로는 분포에서 뺀다', async () => {
      dispatcher.dispatch.mockResolvedValue({
        destination: 'memory://orders',
        partition: null,
        offset: null,
        key: null,
      });

      const result = await service.createBulk(5);

      expect(result.created).toBe(5);
      expect(result.partitionCounts).toEqual({});
    });

    it('파티션을 아는 통로는 그대로 센다', async () => {
      let n = 0;
      dispatcher.dispatch.mockImplementation(async () => ({
        destination: 'orders.created',
        partition: n++ % 2,
        offset: String(n),
        key: 'k',
      }));

      const result = await service.createBulk(4);

      expect(result.partitionCounts).toEqual({ 0: 2, 1: 2 });
    });

    // 실패를 조용히 넘기면 중간에 끊긴 건이 있어도 "완료" 로 보인다.
    it('일부 실패를 건수로 드러내고 합이 맞는다', async () => {
      let n = 0;
      dispatcher.dispatch.mockImplementation(async () => {
        n++;
        if (n === 2) throw new Error('발행 실패');
        return {
          destination: 'orders.created',
          partition: 0,
          offset: String(n),
          key: 'k',
        };
      });

      const result = await service.createBulk(3);

      expect(result.created).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.created + result.failed).toBe(3);
      expect(result.errors).toHaveLength(1);
    });

    // 에러를 전부 담으면 응답이 실패 메시지로 뒤덮인다.
    it('에러 메시지는 다섯 건까지만 담는다', async () => {
      dispatcher.dispatch.mockRejectedValue(new Error('계속 실패'));

      const result = await service.createBulk(20);

      expect(result.failed).toBe(20);
      expect(result.errors).toHaveLength(5);
    });
  });
});
