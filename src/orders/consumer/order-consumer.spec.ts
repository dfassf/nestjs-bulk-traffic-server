import { Kafka } from 'kafkajs';
import {
  ConsumerStats,
  OrderConsumer,
  summarizePerformance,
} from './order-consumer';
import { ConsumerConfig } from './consumer-config';
import { OrderStore } from '../order-store.interface';
import { OrderEventPayload, OrderEventType } from '../order-events';

/** kafkajs consumer.run 에 넘어온 eachMessage 를 붙잡아 직접 호출하기 위한 도구. */
function createKafkaStub() {
  const state: {
    eachMessage?: (payload: any) => Promise<void>;
    runOptions?: any;
    subscriptions: { topic: string; fromBeginning: boolean }[];
    commits: { topic: string; partition: number; offset: string }[];
    connected: boolean;
    disconnected: boolean;
    consumerOptions?: any;
  } = {
    subscriptions: [],
    commits: [],
    connected: false,
    disconnected: false,
  };

  const kafka = {
    consumer: (options: any) => {
      state.consumerOptions = options;
      return {
        connect: async () => {
          state.connected = true;
        },
        disconnect: async () => {
          state.disconnected = true;
        },
        subscribe: async (sub: any) => {
          state.subscriptions.push(sub);
        },
        run: async (options: any) => {
          state.runOptions = options;
          state.eachMessage = options.eachMessage;
        },
        commitOffsets: async (offsets: any[]) => {
          state.commits.push(...offsets);
        },
      };
    },
  } as unknown as Kafka;

  return { kafka, state };
}

function buildConfig(overrides: Partial<ConsumerConfig> = {}): ConsumerConfig {
  return {
    brokers: ['localhost:9092'],
    clientId: 'test-consumer',
    groupId: 'test-group',
    topics: ['orders.created'],
    instances: 1,
    processingDelayMs: 0,
    commitMode: 'after-process',
    commitDelayMs: 0,
    fromBeginning: false,
    crashAfter: 0,
    sessionTimeoutMs: 60000,
    ...overrides,
  };
}

function buildStore(): jest.Mocked<OrderStore> {
  return {
    init: jest.fn(),
    destroy: jest.fn(),
    saveOrder: jest.fn(),
    findOrder: jest.fn(),
    updateStatus: jest.fn(),
    recordEvent: jest.fn(),
    findEvents: jest.fn(),
    findDuplicates: jest.fn(),
    countOrders: jest.fn(),
    countEvents: jest.fn(),
    reset: jest.fn(),
  } as unknown as jest.Mocked<OrderStore>;
}

function buildMessage(
  overrides: Partial<OrderEventPayload> = {},
  offset = '0',
  partition = 0,
) {
  const payload: OrderEventPayload = {
    eventType: OrderEventType.CREATED,
    orderId: 'ord-1',
    userId: 'user-1',
    amount: 10000,
    items: [{ productId: 'prod-A', quantity: 1, unitPrice: 10000 }],
    emittedAt: 1_700_000_000_000,
    ...overrides,
  };

  return {
    topic: 'orders.created',
    partition,
    message: { offset, value: Buffer.from(JSON.stringify(payload)) },
  };
}

describe('OrderConsumer', () => {
  describe('구독', () => {
    it('설정한 토픽을 모두 구독한다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig({ topics: ['orders.created', 'payments.approved'] }),
        buildStore(),
        'c-1',
        {},
        kafka,
      );

      await consumer.start();

      expect(state.subscriptions.map((s) => s.topic)).toEqual([
        'orders.created',
        'payments.approved',
      ]);
      expect(consumer.isRunning()).toBe(true);
    });

    it('fromBeginning 설정을 구독에 넘긴다', async () => {
      const { kafka, state } = createKafkaStub();
      await new OrderConsumer(
        buildConfig({ fromBeginning: true }),
        buildStore(),
        'c-1',
        {},
        kafka,
      ).start();

      expect(state.subscriptions[0].fromBeginning).toBe(true);
    });

    // 자동 커밋을 켜두면 kafkajs 가 백그라운드에서 알아서 커밋해버려
    // 커밋 시점을 제어할 수 없다. 중복·유실 실험이 성립하지 않는다.
    it('자동 커밋을 끄고 수동으로 커밋한다', async () => {
      const { kafka, state } = createKafkaStub();
      await new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      ).start();

      expect(state.runOptions.autoCommit).toBe(false);
    });

    it('그룹 이름을 카프카에 넘긴다', async () => {
      const { kafka, state } = createKafkaStub();
      await new OrderConsumer(
        buildConfig({ groupId: 'analytics' }),
        buildStore(),
        'c-1',
        {},
        kafka,
      ).start();

      expect(state.consumerOptions.groupId).toBe('analytics');
    });
  });

  describe('처리와 기록', () => {
    it('이벤트를 파티션·오프셋·컨슈머와 함께 기록한다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      await new OrderConsumer(buildConfig(), store, 'c-7', {}, kafka).start();

      await state.eachMessage!(buildMessage({ orderId: 'ord-9' }, '42', 3));

      expect(store.recordEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          orderId: 'ord-9',
          eventType: OrderEventType.CREATED,
          topic: 'orders.created',
          partition: 3,
          offset: '42',
          consumerId: 'c-7',
        }),
      );
    });

    it('처리 건수와 파티션 분포를 센다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!(buildMessage({}, '0', 0));
      await state.eachMessage!(buildMessage({}, '1', 0));
      await state.eachMessage!(buildMessage({}, '2', 1));

      const stats = consumer.getStats();
      expect(stats.processed).toBe(3);
      expect(stats.partitionCounts).toEqual({ 0: 2, 1: 1 });
    });

    // 중복을 막으면 실험에서 보려는 현상이 사라진다.
    it('같은 오프셋이 두 번 와도 두 번 기록한다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      await new OrderConsumer(buildConfig(), store, 'c-1', {}, kafka).start();

      await state.eachMessage!(buildMessage({}, '5'));
      await state.eachMessage!(buildMessage({}, '5'));

      expect(store.recordEvent).toHaveBeenCalledTimes(2);
    });
  });

  describe('커밋 시점', () => {
    // 카프카는 "다음에 읽을 위치" 를 기록한다. offset 을 그대로 넘기면
    // 재시작 때 같은 건을 또 읽는다.
    it('처리한 오프셋 + 1 을 커밋한다', async () => {
      const { kafka, state } = createKafkaStub();
      await new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      ).start();

      await state.eachMessage!(buildMessage({}, '10', 2));

      expect(state.commits).toEqual([
        { topic: 'orders.created', partition: 2, offset: '11' },
      ]);
    });

    it('after-process 는 기록 뒤에 커밋한다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      const order: string[] = [];
      store.recordEvent.mockImplementation(async () => {
        order.push('record');
      });
      const consumer = new OrderConsumer(
        buildConfig({ commitMode: 'after-process' }),
        store,
        'c-1',
        {},
        kafka,
      );
      await consumer.start();
      const originalCommit = state.commits;
      Object.defineProperty(state, 'commits', {
        get: () => originalCommit,
      });

      await state.eachMessage!(buildMessage());

      expect(order).toEqual(['record']);
      expect(state.commits).toHaveLength(1);
    });

    // 처리 전에 커밋하면 그 사이에 죽었을 때 그 건은 영영 처리되지 않는다(유실).
    it('before-process 는 기록 전에 커밋한다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      let committedWhenRecording = 0;
      store.recordEvent.mockImplementation(async () => {
        committedWhenRecording = state.commits.length;
      });

      await new OrderConsumer(
        buildConfig({ commitMode: 'before-process' }),
        store,
        'c-1',
        {},
        kafka,
      ).start();

      await state.eachMessage!(buildMessage());

      expect(committedWhenRecording).toBe(1);
    });

    it('처리가 실패해도 after-process 는 커밋한다 (재처리 대신 넘어감)', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      store.recordEvent.mockRejectedValue(new Error('저장 실패'));
      const consumer = new OrderConsumer(
        buildConfig(),
        store,
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!(buildMessage());

      expect(consumer.getStats().failed).toBe(1);
      expect(state.commits).toHaveLength(1);
    });
  });

  describe('강제 종료 지점', () => {
    // 지정 건수에 도달하면 커밋하지 않고 이 컨슈머만 빠진다.
    // "처리했지만 커밋 못 한" 상태가 되어 재분배 후 중복이 관측된다.
    it('지정 건수에 도달하면 커밋하지 않고 알린다', async () => {
      const { kafka, state } = createKafkaStub();
      const onCrashPoint = jest.fn();
      await new OrderConsumer(
        buildConfig({ crashAfter: 2 }),
        buildStore(),
        'c-1',
        { onCrashPoint },
        kafka,
      ).start();

      await state.eachMessage!(buildMessage({}, '0'));
      expect(onCrashPoint).not.toHaveBeenCalled();
      expect(state.commits).toHaveLength(1);

      await state.eachMessage!(buildMessage({}, '1'));

      expect(onCrashPoint).toHaveBeenCalledTimes(1);
      // 두 번째 건은 커밋되지 않아야 재분배 후 다시 읽힌다.
      expect(state.commits).toHaveLength(1);
    });

    // 예전에는 여기서 프로세스를 통째로 끝냈다. 그러면 같은 프로세스의 다른
    // 컨슈머들이 커밋 직전에 함께 죽어, 실제로는 없었을 중복까지 관측됐다.
    // 이 컨슈머만 그룹에서 빠져야 남은 컨슈머가 파티션을 넘겨받는다.
    it('빠질 때 자기 연결만 끊고 알림보다 먼저 끊는다', async () => {
      const { kafka, state } = createKafkaStub();
      const disconnectedWhenNotified: boolean[] = [];
      const consumer = new OrderConsumer(
        buildConfig({ crashAfter: 1 }),
        buildStore(),
        'c-1',
        {
          onCrashPoint: () => disconnectedWhenNotified.push(state.disconnected),
        },
        kafka,
      );
      await consumer.start();

      await state.eachMessage!(buildMessage({}, '0'));

      // 알림을 받은 시점에 이미 그룹에서 빠져 있어야 한다. 알림을 받고 나서
      // 끊으면 그 사이에 메시지를 더 받아 실험 건수가 어긋난다.
      expect(disconnectedWhenNotified).toEqual([true]);
      expect(consumer.isRunning()).toBe(false);
      expect(consumer.hasCrashed()).toBe(true);
    });

    it('정상 종료는 빠진 것으로 세지 않는다', async () => {
      const { kafka } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig({ crashAfter: 0 }),
        buildStore(),
        'c-1',
        {},
        kafka,
      );
      await consumer.start();
      await consumer.stop();

      // 둘 다 멈춘 상태지만 이유가 다르다. 섞이면 중복 원인을 잘못 읽는다.
      expect(consumer.isRunning()).toBe(false);
      expect(consumer.hasCrashed()).toBe(false);
    });

    it('crashAfter 가 0 이면 계속 처리한다', async () => {
      const { kafka, state } = createKafkaStub();
      const onCrashPoint = jest.fn();
      await new OrderConsumer(
        buildConfig({ crashAfter: 0 }),
        buildStore(),
        'c-1',
        { onCrashPoint },
        kafka,
      ).start();

      await state.eachMessage!(buildMessage({}, '0'));
      await state.eachMessage!(buildMessage({}, '1'));

      expect(onCrashPoint).not.toHaveBeenCalled();
      expect(state.commits).toHaveLength(2);
    });
  });

  describe('잘못된 메시지', () => {
    it('본문이 비면 실패로 세고 넘어간다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      const consumer = new OrderConsumer(
        buildConfig(),
        store,
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!({
        topic: 'orders.created',
        partition: 0,
        message: { offset: '0', value: null },
      });

      expect(consumer.getStats().failed).toBe(1);
      expect(store.recordEvent).not.toHaveBeenCalled();
    });

    it('JSON 이 깨지면 실패로 센다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!({
        topic: 'orders.created',
        partition: 0,
        message: { offset: '0', value: Buffer.from('{깨진 json') },
      });

      expect(consumer.getStats().failed).toBe(1);
    });

    // 빈 값으로 메우면 어느 주문인지 모르는 기록이 쌓인다.
    it('orderId 가 없으면 실패로 센다', async () => {
      const { kafka, state } = createKafkaStub();
      const store = buildStore();
      const consumer = new OrderConsumer(
        buildConfig(),
        store,
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!({
        topic: 'orders.created',
        partition: 0,
        message: {
          offset: '0',
          value: Buffer.from(
            JSON.stringify({ eventType: OrderEventType.CREATED }),
          ),
        },
      });

      expect(consumer.getStats().failed).toBe(1);
      expect(store.recordEvent).not.toHaveBeenCalled();
    });

    it('알 수 없는 이벤트 종류는 실패로 센다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await state.eachMessage!({
        topic: 'orders.created',
        partition: 0,
        message: {
          offset: '0',
          value: Buffer.from(
            JSON.stringify({ orderId: 'ord-1', eventType: 'orders.unknown' }),
          ),
        },
      });

      expect(consumer.getStats().failed).toBe(1);
    });
  });

  describe('종료', () => {
    it('stop 하면 연결을 끊는다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      );
      await consumer.start();

      await consumer.stop();

      expect(state.disconnected).toBe(true);
      expect(consumer.isRunning()).toBe(false);
    });

    it('시작 전에 stop 해도 문제없다', async () => {
      const { kafka, state } = createKafkaStub();
      const consumer = new OrderConsumer(
        buildConfig(),
        buildStore(),
        'c-1',
        {},
        kafka,
      );

      await consumer.stop();

      expect(state.disconnected).toBe(false);
    });
  });
});

/**
 * Go 컨슈머(go-engine/internal/consumer/stats.go)와 같은 규칙이어야
 * 두 런타임을 나란히 비교할 수 있다. 한쪽만 기준이 다르면 숫자가
 * 달라도 그게 성능 차이인지 계산 차이인지 알 수 없다.
 */
describe('summarizePerformance', () => {
  function buildStats(overrides: Partial<ConsumerStats> = {}): ConsumerStats {
    return {
      consumerId: 'c-1',
      processed: 0,
      failed: 0,
      partitionCounts: {},
      startedAt: 1_000,
      firstProcessedAt: null,
      lastProcessedAt: null,
      latenciesMs: [],
      ...overrides,
    };
  }

  // 한 건도 처리 못 했는데 0ms 로 내보내면 가장 빠른 결과처럼 읽힌다.
  it('표본이 없으면 지연·처리량이 0 이 아니라 null 이다', () => {
    const perf = summarizePerformance(buildStats());

    expect(perf.avgMs).toBeNull();
    expect(perf.p50Ms).toBeNull();
    expect(perf.p95Ms).toBeNull();
    expect(perf.throughputPerSec).toBeNull();
  });

  it('전량 실패해도 지연은 null 이다', () => {
    const perf = summarizePerformance(buildStats({ failed: 3 }));

    expect(perf.failed).toBe(3);
    expect(perf.avgMs).toBeNull();
    expect(perf.throughputPerSec).toBeNull();
  });

  // 분위수를 내림으로 잡으면 p95 가 실제보다 낮게 나와 느린 꼬리가 가려진다.
  it('분위수 위치를 올림으로 잡는다', () => {
    const perf = summarizePerformance(
      buildStats({
        processed: 5,
        latenciesMs: [50, 10, 90, 20, 30],
      }),
    );

    // 정렬하면 10·20·30·50·90. Go 쪽과 같은 값이 나와야 한다.
    expect(perf.p50Ms).toBe(30);
    expect(perf.p95Ms).toBe(90);
  });

  // 처리량은 '일한 시간'으로 나눈다. 기다리며 논 시간을 넣으면
  // 처리가 끝난 뒤에도 숫자가 계속 나빠진다.
  it('처리량은 첫 처리부터 마지막 처리까지로 잰다', () => {
    const perf = summarizePerformance(
      buildStats({
        processed: 100,
        // 컨슈머는 1초에 떴지만 첫 건은 10초에 처리했다.
        startedAt: 1_000,
        firstProcessedAt: 10_000,
        lastProcessedAt: 11_000,
        latenciesMs: Array(100).fill(1),
      }),
    );

    // 일한 시간 1초에 100건 → 100건/초.
    // 뜬 시각부터 셌다면 100/10 = 10건/초로 나온다.
    expect(perf.throughputPerSec).toBe(100);
  });

  it('한 건뿐이면 처리량은 측정 불가다', () => {
    const perf = summarizePerformance(
      buildStats({
        processed: 1,
        firstProcessedAt: 10_000,
        lastProcessedAt: 10_000,
        latenciesMs: [5],
      }),
    );

    // 잰 구간이 0 이라 나눌 수 없다. 0 으로 메우면 '처리량 0'으로 보인다.
    expect(perf.throughputPerSec).toBeNull();
    // 지연은 표본이 있으니 나와야 한다.
    expect(perf.avgMs).toBe(5);
  });

  it('평균은 느린 건을 반영한다', () => {
    const perf = summarizePerformance(
      buildStats({
        processed: 4,
        latenciesMs: [1, 1, 1, 397],
      }),
    );

    expect(perf.avgMs).toBe(100);
  });
});
