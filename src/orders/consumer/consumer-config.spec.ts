import { consumerConfigFromEnv, describeConfig } from './consumer-config';
import { ORDER_TOPICS } from '../order-events';

describe('consumerConfigFromEnv', () => {
  const keys = [
    'CONSUMER_CLIENT_ID',
    'CONSUMER_GROUP_ID',
    'CONSUMER_TOPICS',
    'CONSUMER_COUNT',
    'CONSUMER_DELAY_MS',
    'CONSUMER_COMMIT_MODE',
    'CONSUMER_COMMIT_DELAY_MS',
    'CONSUMER_FROM_BEGINNING',
    'CONSUMER_CRASH_AFTER',
    'KAFKA_BROKERS',
  ];

  afterEach(() => keys.forEach((k) => delete process.env[k]));

  describe('기본값', () => {
    it('컨슈머 1개, 지연 없음, 처리 후 커밋이 기본이다', () => {
      const config = consumerConfigFromEnv();

      expect(config.instances).toBe(1);
      expect(config.processingDelayMs).toBe(0);
      expect(config.commitMode).toBe('after-process');
      expect(config.commitDelayMs).toBe(0);
      expect(config.fromBeginning).toBe(false);
      expect(config.crashAfter).toBe(0);
    });

    it('주문 토픽 전부를 구독한다', () => {
      expect(consumerConfigFromEnv().topics).toEqual(
        Object.values(ORDER_TOPICS),
      );
    });

    it('그룹 이름 기본값이 있다', () => {
      expect(consumerConfigFromEnv().groupId).toBe('order-processor');
    });
  });

  describe('실험 스위치', () => {
    it('인스턴스 수를 늘린다 (파티션 분배 실험)', () => {
      process.env.CONSUMER_COUNT = '3';
      expect(consumerConfigFromEnv().instances).toBe(3);
    });

    it('처리 속도를 늦춘다 (Lag 실험)', () => {
      process.env.CONSUMER_DELAY_MS = '500';
      expect(consumerConfigFromEnv().processingDelayMs).toBe(500);
    });

    it('처리 전 커밋으로 바꾼다 (유실 실험)', () => {
      process.env.CONSUMER_COMMIT_MODE = 'before-process';
      expect(consumerConfigFromEnv().commitMode).toBe('before-process');
    });

    it('커밋 지연을 준다 (중복 창 넓히기)', () => {
      process.env.CONSUMER_COMMIT_DELAY_MS = '2000';
      expect(consumerConfigFromEnv().commitDelayMs).toBe(2000);
    });

    it('처음부터 읽는다 (과거 재생 실험)', () => {
      process.env.CONSUMER_FROM_BEGINNING = 'true';
      expect(consumerConfigFromEnv().fromBeginning).toBe(true);
    });

    it('지정 건수 후 강제 종료를 예약한다', () => {
      process.env.CONSUMER_CRASH_AFTER = '50';
      expect(consumerConfigFromEnv().crashAfter).toBe(50);
    });

    it('그룹을 바꾼다 (여러 그룹이 같은 이벤트를 보는 실험)', () => {
      process.env.CONSUMER_GROUP_ID = 'analytics';
      expect(consumerConfigFromEnv().groupId).toBe('analytics');
    });

    it('구독 토픽을 좁힌다', () => {
      process.env.CONSUMER_TOPICS = 'orders.created, payments.approved';
      expect(consumerConfigFromEnv().topics).toEqual([
        'orders.created',
        'payments.approved',
      ]);
    });
  });

  describe('잘못된 값', () => {
    // 오타를 기본값으로 흡수하면 어떤 방식으로 돌고 있는지 모른 채 실험하게 된다.
    it('커밋 방식 오타는 기본값으로 넘기지 않고 예외를 던진다', () => {
      process.env.CONSUMER_COMMIT_MODE = 'after';
      expect(() => consumerConfigFromEnv()).toThrow(
        /after-process 또는 before-process/,
      );
    });

    it('빈 커밋 방식은 기본값을 쓴다', () => {
      process.env.CONSUMER_COMMIT_MODE = '';
      expect(consumerConfigFromEnv().commitMode).toBe('after-process');
    });

    it('음수 지연은 예외를 던진다', () => {
      process.env.CONSUMER_DELAY_MS = '-1';
      expect(() => consumerConfigFromEnv()).toThrow(/0 이상 정수/);
    });

    it('숫자가 아닌 지연은 예외를 던진다', () => {
      process.env.CONSUMER_DELAY_MS = '느리게';
      expect(() => consumerConfigFromEnv()).toThrow(/0 이상 정수/);
    });

    it('0 은 지연 없음으로 허용한다', () => {
      process.env.CONSUMER_DELAY_MS = '0';
      expect(consumerConfigFromEnv().processingDelayMs).toBe(0);
    });

    it('구독 토픽이 비면 예외를 던진다', () => {
      process.env.CONSUMER_TOPICS = ' , , ';
      expect(() => consumerConfigFromEnv()).toThrow(/토픽이 하나도 없습니다/);
    });
  });
});

describe('describeConfig', () => {
  it('기본 설정을 짧게 요약한다', () => {
    const text = describeConfig({
      brokers: ['localhost:9092'],
      clientId: 'c',
      groupId: 'order-processor',
      topics: ['orders.created'],
      instances: 1,
      processingDelayMs: 0,
      commitMode: 'after-process',
      commitDelayMs: 0,
      fromBeginning: false,
      crashAfter: 0,
    });

    expect(text).toContain('그룹=order-processor');
    expect(text).toContain('인스턴스=1');
    expect(text).toContain('커밋=after-process');
    expect(text).not.toContain('처리지연');
  });

  it('켜진 실험 스위치만 덧붙인다', () => {
    const text = describeConfig({
      brokers: ['localhost:9092'],
      clientId: 'c',
      groupId: 'g',
      topics: ['orders.created'],
      instances: 3,
      processingDelayMs: 200,
      commitMode: 'before-process',
      commitDelayMs: 1000,
      fromBeginning: true,
      crashAfter: 50,
    });

    expect(text).toContain('처리지연=200ms');
    expect(text).toContain('커밋지연=1000ms');
    expect(text).toContain('처음부터');
    expect(text).toContain('50건 후 강제종료');
  });
});
