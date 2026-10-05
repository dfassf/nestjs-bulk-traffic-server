import { MemoryOrderDispatcher } from './memory-order.dispatcher';
import { SqliteOrderStore } from '../sqlite-order.store';
import { OrderEventPayload, OrderEventType } from '../order-events';

/**
 * 저장소는 실물(메모리 SQLite)을 쓴다.
 *
 * 이 통로가 하는 일은 "큐에서 꺼내 기록한다" 가 전부라, 기록을 가짜로 두면
 * 정말 쌓였는지가 아니라 함수를 불렀는지만 확인하게 된다.
 */
describe('MemoryOrderDispatcher', () => {
  let store: SqliteOrderStore;
  let dispatcher: MemoryOrderDispatcher;

  beforeEach(async () => {
    store = new SqliteOrderStore(':memory:');
    await store.init();
    dispatcher = new MemoryOrderDispatcher(store);
  });

  afterEach(async () => {
    await store.destroy();
  });

  function buildEvent(orderId: string): OrderEventPayload {
    return {
      eventType: OrderEventType.CREATED,
      orderId,
      userId: 'user-1',
      amount: 10000,
      items: [{ productId: 'p', quantity: 1, unitPrice: 10000 }],
      emittedAt: Date.now(),
    };
  }

  /** 큐를 비우는 루프가 마이크로태스크로 돌므로 끝날 때까지 기다린다. */
  const settle = () => new Promise((resolve) => setImmediate(resolve));

  describe('내보내기', () => {
    it('큐에 쌓은 뒤 기록까지 남긴다', async () => {
      await dispatcher.dispatch(buildEvent('ord-1'));
      await settle();

      expect(await store.countEvents()).toBe(1);
      expect(dispatcher.getStats().processed).toBe(1);
    });

    /**
     * 카프카는 파티션·오프셋이 있지만 프로세스 안의 큐에는 그 개념이 없다.
     * 없는 값을 0 으로 채우면 "0번 파티션" 과 구분되지 않는다.
     */
    it('파티션·오프셋을 0 이 아니라 null 로 돌려준다', async () => {
      const result = await dispatcher.dispatch(buildEvent('ord-1'));

      expect(result.partition).toBeNull();
      expect(result.offset).toBeNull();
      expect(result.destination).toBe('memory://orders');
    });

    it('어느 수단인지 이름으로 드러낸다', () => {
      expect(dispatcher.name).toBe('memory');
    });

    it('여러 건을 순서대로 처리한다', async () => {
      for (const id of ['ord-1', 'ord-2', 'ord-3']) {
        await dispatcher.dispatch(buildEvent(id));
      }
      await settle();

      const events = await store.findEvents();
      expect(events).toHaveLength(3);
      expect(dispatcher.getStats().processed).toBe(3);
    });
  });

  describe('카프카와 갈리는 지점', () => {
    /**
     * 카프카 소비 기록과 섞이면 그룹별 집계가 뒤엉킨다.
     * 파티션을 -1 로 두어 "파티션 없는 방식" 임을 남긴다.
     */
    it('기록을 카프카 것과 구분되게 남긴다', async () => {
      await dispatcher.dispatch(buildEvent('ord-1'));
      await settle();

      const [event] = await store.findEvents();
      expect(event.partition).toBe(-1);
      expect(event.consumerId).toMatch(/^memory\//);
    });

    /**
     * 꺼낸 메시지는 큐에서 사라진다. 처리에 실패해도 돌려놓을 자리가 없다.
     * 카프카라면 커밋하지 않고 다시 읽으면 되는데 여기서는 그 건이 없어진다.
     */
    it('처리에 실패한 건은 되돌아오지 않고 사라진다', async () => {
      jest
        .spyOn(store, 'recordEvent')
        .mockRejectedValueOnce(new Error('저장 실패'));

      await dispatcher.dispatch(buildEvent('ord-1'));
      await settle();

      const stats = dispatcher.getStats();
      expect(stats.failed).toBe(1);
      expect(stats.processed).toBe(0);
      // 큐가 비었다. 실패한 건을 다시 시도할 방법이 없다.
      expect(stats.pending).toBe(0);
      expect(await store.countEvents()).toBe(0);
    });

    /**
     * 프로세스가 멈추면 아직 처리하지 않은 것은 같이 사라진다.
     * 디스크에 남는 카프카와 달리 재개할 자리가 없다.
     */
    it('멈춘 뒤에는 새로 받지 않는다', async () => {
      await dispatcher.onModuleDestroy();

      expect(dispatcher.isReady()).toBe(false);
      await expect(dispatcher.dispatch(buildEvent('ord-1'))).rejects.toThrow(
        /이미 멈춰 있습니다/,
      );
    });

    it('남은 양을 보여준다 (프로세스가 죽으면 이만큼 사라진다)', async () => {
      // 저장을 느리게 만들어 큐에 쌓이는 상태를 만든다.
      let release: (() => void) | undefined;
      jest.spyOn(store, 'recordEvent').mockImplementation(
        () =>
          new Promise<void>((resolve) => {
            release = () => resolve();
          }),
      );

      await dispatcher.dispatch(buildEvent('ord-1'));
      await dispatcher.dispatch(buildEvent('ord-2'));
      await dispatcher.dispatch(buildEvent('ord-3'));
      await settle();

      // 첫 건이 처리 중이고 나머지는 큐에서 대기한다.
      expect(dispatcher.getStats().pending).toBe(2);
      expect(dispatcher.getStats().processed).toBe(0);

      release?.();
    });
  });

  describe('집계', () => {
    it('넣은 건수와 처리한 건수를 따로 센다', async () => {
      await dispatcher.dispatch(buildEvent('ord-1'));
      await dispatcher.dispatch(buildEvent('ord-2'));
      await settle();

      const stats = dispatcher.getStats();
      expect(stats.enqueued).toBe(2);
      expect(stats.processed).toBe(2);
      // 넣은 것과 처리한 것의 차이가 유실 가능한 양이다.
      expect(stats.enqueued - stats.processed - stats.failed).toBe(
        stats.pending,
      );
    });
  });
});
