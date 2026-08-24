import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { SqliteOrderStore } from './sqlite-order.store';
import {
  Order,
  OrderEventRecord,
  OrderEventType,
  OrderStatus,
} from './order-events';

/**
 * 실제 SQLite 파일로 검증한다.
 * mock 으로 통과하는 것과 실제 저장·조회가 맞는 것은 다르다.
 */
describe('SqliteOrderStore', () => {
  let store: SqliteOrderStore;
  let dbPath: string;

  function buildOrder(overrides: Partial<Order> = {}): Order {
    const now = Date.now();
    return {
      orderId: 'ord-1',
      userId: 'user-1',
      amount: 10000,
      items: [{ productId: 'prod-A', quantity: 2, unitPrice: 5000 }],
      status: OrderStatus.CREATED,
      createdAt: now,
      updatedAt: now,
      ...overrides,
    };
  }

  function buildEvent(
    overrides: Partial<OrderEventRecord> = {},
  ): OrderEventRecord {
    return {
      orderId: 'ord-1',
      eventType: OrderEventType.CREATED,
      topic: 'orders.created',
      partition: 0,
      offset: '10',
      consumerId: 'consumer-1',
      consumedAt: Date.now(),
      ...overrides,
    };
  }

  beforeEach(async () => {
    dbPath = path.join(
      fs.mkdtempSync(path.join(os.tmpdir(), 'order-store-')),
      'orders.sqlite',
    );
    store = new SqliteOrderStore(dbPath);
    await store.init();
  });

  afterEach(async () => {
    await store.destroy();
    fs.rmSync(path.dirname(dbPath), { recursive: true, force: true });
  });

  // ':memory:' 는 경로가 아니라 "파일 대신 메모리에 두라" 는 특수 값이다.
  // 경로로 취급해 절대경로로 바꾸면 그 이름의 파일이 실제로 생기고,
  // 메모리에서 도는 줄 알았던 테스트가 파일에 쌓이며 서로 간섭한다.
  describe('메모리 DB 경로', () => {
    it("':memory:' 는 파일을 만들지 않는다", async () => {
      const before = fs.existsSync(path.resolve(':memory:'));
      const memoryStore = new SqliteOrderStore(':memory:');
      await memoryStore.init();

      try {
        expect(memoryStore.isInMemory()).toBe(true);

        // 실제로 쓰고 읽어도 파일이 생기면 안 된다.
        await memoryStore.saveOrder(buildOrder());
        expect(await memoryStore.findOrder('ord-1')).not.toBeNull();
        expect(fs.existsSync(path.resolve(':memory:'))).toBe(before);
      } finally {
        await memoryStore.destroy();
      }
    });

    it('메모리 DB 끼리는 서로 격리된다', async () => {
      const a = new SqliteOrderStore(':memory:');
      const b = new SqliteOrderStore(':memory:');
      await a.init();
      await b.init();

      try {
        await a.saveOrder(buildOrder());

        // 같은 파일을 공유했다면 b 에서도 보인다. 그러면 테스트가 서로 오염된다.
        expect(await b.findOrder('ord-1')).toBeNull();
      } finally {
        await a.destroy();
        await b.destroy();
      }
    });

    it('보통 경로는 절대경로 파일로 다룬다', async () => {
      const fileStore = new SqliteOrderStore(dbPath);
      expect(fileStore.isInMemory()).toBe(false);
    });
  });

  describe('주문 저장·조회', () => {
    it('저장한 주문을 그대로 읽어온다', async () => {
      const order = buildOrder();
      await store.saveOrder(order);

      const found = await store.findOrder('ord-1');

      expect(found).not.toBeNull();
      expect(found!.orderId).toBe('ord-1');
      expect(found!.userId).toBe('user-1');
      expect(found!.amount).toBe(10000);
      expect(found!.status).toBe(OrderStatus.CREATED);
    });

    // 메모리에서 같은지만 보면 직렬화 왕복 불일치를 못 잡는다.
    it('품목 배열이 저장·조회 왕복 후에도 같다', async () => {
      const items = [
        { productId: 'prod-A', quantity: 2, unitPrice: 5000 },
        { productId: 'prod-B', quantity: 1, unitPrice: 3000 },
      ];
      await store.saveOrder(buildOrder({ items }));

      const found = await store.findOrder('ord-1');

      expect(found!.items).toEqual(items);
    });

    it('없는 주문은 null 을 돌려준다', async () => {
      expect(await store.findOrder('없음')).toBeNull();
    });

    it('상태를 바꾸면 updatedAt 도 갱신된다', async () => {
      const order = buildOrder({ updatedAt: 1_700_000_000_000 });
      await store.saveOrder(order);

      await store.updateStatus('ord-1', OrderStatus.PAYMENT_APPROVED);

      const found = await store.findOrder('ord-1');
      expect(found!.status).toBe(OrderStatus.PAYMENT_APPROVED);
      expect(found!.updatedAt).toBeGreaterThan(1_700_000_000_000);
    });

    // 조용히 넘어가면 상태가 어긋난 걸 아무도 모른다.
    it('없는 주문의 상태를 바꾸려 하면 예외를 던진다', async () => {
      await expect(
        store.updateStatus('없음', OrderStatus.SHIPPED),
      ).rejects.toThrow(/주문을 찾을 수 없어/);
    });
  });

  describe('이벤트 기록', () => {
    it('파티션·오프셋·컨슈머를 함께 남긴다', async () => {
      await store.recordEvent(
        buildEvent({ partition: 2, offset: '42', consumerId: 'c-3' }),
      );

      const events = await store.findEvents('ord-1');

      expect(events).toHaveLength(1);
      expect(events[0].partition).toBe(2);
      expect(events[0].offset).toBe('42');
      expect(events[0].consumerId).toBe('c-3');
    });

    // 중복을 막으면 실험에서 보려는 현상이 사라진다. 일부러 안 막는다.
    it('같은 이벤트를 두 번 기록하면 두 행이 쌓인다', async () => {
      await store.recordEvent(buildEvent({ offset: '10' }));
      await store.recordEvent(buildEvent({ offset: '10' }));

      const events = await store.findEvents('ord-1');
      expect(events).toHaveLength(2);
    });

    it('주문별 이벤트는 소비 순서대로 나온다', async () => {
      await store.recordEvent(
        buildEvent({ eventType: OrderEventType.CREATED, consumedAt: 1000 }),
      );
      await store.recordEvent(
        buildEvent({
          eventType: OrderEventType.PAYMENT_APPROVED,
          consumedAt: 3000,
        }),
      );
      await store.recordEvent(
        buildEvent({
          eventType: OrderEventType.INVENTORY_RESERVED,
          consumedAt: 2000,
        }),
      );

      const events = await store.findEvents('ord-1');

      expect(events.map((e) => e.eventType)).toEqual([
        OrderEventType.CREATED,
        OrderEventType.INVENTORY_RESERVED,
        OrderEventType.PAYMENT_APPROVED,
      ]);
    });
  });

  describe('중복 집계', () => {
    it('두 번 이상 처리된 주문·이벤트를 건수와 함께 돌려준다', async () => {
      await store.recordEvent(buildEvent({ orderId: 'ord-1' }));
      await store.recordEvent(buildEvent({ orderId: 'ord-1' }));
      await store.recordEvent(buildEvent({ orderId: 'ord-1' }));
      await store.recordEvent(buildEvent({ orderId: 'ord-2' }));

      const duplicates = await store.findDuplicates();

      expect(duplicates).toHaveLength(1);
      expect(duplicates[0].orderId).toBe('ord-1');
      expect(duplicates[0].count).toBe(3);
    });

    it('중복이 없으면 빈 배열이다', async () => {
      await store.recordEvent(buildEvent({ orderId: 'ord-1' }));
      await store.recordEvent(buildEvent({ orderId: 'ord-2' }));

      expect(await store.findDuplicates()).toEqual([]);
    });

    it('같은 주문이라도 이벤트 종류가 다르면 중복이 아니다', async () => {
      await store.recordEvent(
        buildEvent({ eventType: OrderEventType.CREATED }),
      );
      await store.recordEvent(
        buildEvent({ eventType: OrderEventType.PAYMENT_APPROVED }),
      );

      expect(await store.findDuplicates()).toEqual([]);
    });
  });

  describe('집계·초기화', () => {
    it('주문 수와 이벤트 수를 센다', async () => {
      await store.saveOrder(buildOrder({ orderId: 'ord-1' }));
      await store.saveOrder(buildOrder({ orderId: 'ord-2' }));
      await store.recordEvent(buildEvent({ orderId: 'ord-1' }));

      expect(await store.countOrders()).toBe(2);
      expect(await store.countEvents()).toBe(1);
    });

    it('초기화하면 주문과 이벤트가 모두 비워진다', async () => {
      await store.saveOrder(buildOrder());
      await store.recordEvent(buildEvent());

      await store.reset();

      expect(await store.countOrders()).toBe(0);
      expect(await store.countEvents()).toBe(0);
    });
  });
});
