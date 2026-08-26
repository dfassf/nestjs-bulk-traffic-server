import {
  formatConsumerLine,
  formatMs,
  formatPartitions,
  formatReport,
  formatThroughput,
} from './consumer-report';
import { ConsumerStats } from './order-consumer';

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

describe('formatMs', () => {
  // 0ms 는 가장 좋아 보이는 값이라, 한 건도 처리 못 한 결과가
  // 가장 빠른 것처럼 읽힌다.
  it('측정 못 한 값은 0 이 아니라 측정 불가로 보여준다', () => {
    expect(formatMs(null)).toBe('측정 불가');
    expect(formatMs(null)).not.toContain('0');
  });

  it('값이 있으면 소수점 세 자리까지 보여준다', () => {
    expect(formatMs(1.2345)).toBe('1.234ms');
  });

  // 진짜 0ms 와 측정 불가는 다른 상태다. 뭉개면 구분이 사라진다.
  it('실제 0 은 측정 불가와 다르게 보여준다', () => {
    expect(formatMs(0)).toBe('0.000ms');
    expect(formatMs(0)).not.toBe(formatMs(null));
  });
});

describe('formatThroughput', () => {
  it('측정 못 한 값은 0 이 아니라 측정 불가로 보여준다', () => {
    expect(formatThroughput(null)).toBe('측정 불가');
  });

  it('값이 있으면 건/초로 보여준다', () => {
    expect(formatThroughput(146.94)).toBe('146.9건/초');
  });

  it('실제 0 은 측정 불가와 다르게 보여준다', () => {
    expect(formatThroughput(0)).toBe('0.0건/초');
    expect(formatThroughput(0)).not.toBe(formatThroughput(null));
  });
});

describe('formatPartitions', () => {
  // 파티션이 하나도 없으면 이 컨슈머가 놀고 있다는 뜻이다.
  // 빈 문자열로 두면 왜 처리가 0 인지 읽을 수 없다.
  it('할당된 파티션이 없으면 그 사실을 알린다', () => {
    expect(formatPartitions({})).toBe('(할당된 파티션 없음)');
  });

  it('파티션 번호 순으로 정렬해 보여준다', () => {
    expect(formatPartitions({ 5: 39, 0: 32, 3: 41 })).toBe('p0:32 p3:41 p5:39');
  });
});

describe('formatConsumerLine', () => {
  it('처리 건수와 파티션 분포, 성능을 한 줄로 보여준다', () => {
    const line = formatConsumerLine(
      buildStats({
        consumerId: '801-0',
        processed: 112,
        partitionCounts: { 0: 32, 3: 41, 5: 39 },
        firstProcessedAt: 10_000,
        lastProcessedAt: 11_000,
        latenciesMs: Array(112).fill(2),
      }),
      false,
    );

    expect(line).toContain('801-0');
    expect(line).toContain('처리=112');
    expect(line).toContain('p0:32 p3:41 p5:39');
    expect(line).toContain('평균=2.000ms');
    expect(line).toContain('112.0건/초');
  });

  // 빠진 컨슈머를 표시하지 않으면 처리량이 왜 줄었는지 읽을 수 없다.
  it('커밋 없이 빠진 컨슈머는 그 사실을 표시한다', () => {
    const line = formatConsumerLine(buildStats({ processed: 5 }), true);

    expect(line).toContain('[커밋 없이 빠짐]');
  });

  it('정상 컨슈머에는 빠짐 표시가 없다', () => {
    const line = formatConsumerLine(buildStats({ processed: 5 }), false);

    expect(line).not.toContain('커밋 없이 빠짐');
  });

  // 한 건도 처리 못 한 컨슈머가 가장 빠른 것처럼 보이면 안 된다.
  it('처리 건이 없으면 지연·처리량을 측정 불가로 보여준다', () => {
    const line = formatConsumerLine(buildStats(), false);

    expect(line).toContain('평균=측정 불가');
    expect(line).toContain('p95=측정 불가');
    expect(line).toContain('측정 불가');
    expect(line).not.toContain('0.000ms');
    expect(line).not.toContain('0.0건/초');
  });
});

describe('formatReport', () => {
  // 매 주기마다 "0건" 을 찍으면 정작 봐야 할 줄이 묻힌다.
  it('아직 아무 일도 없었으면 아무것도 내보내지 않는다', () => {
    const lines = formatReport([
      { stats: buildStats(), crashed: false },
      { stats: buildStats({ consumerId: 'c-2' }), crashed: false },
    ]);

    expect(lines).toEqual([]);
  });

  // 전량 실패도 보고해야 한다. 처리가 0 이라고 조용히 넘기면
  // 작업이 터지고 있는데 화면에는 아무것도 안 나온다.
  it('처리가 0 이어도 실패가 있으면 보고한다', () => {
    const lines = formatReport([
      { stats: buildStats({ failed: 7 }), crashed: false },
    ]);

    expect(lines[0]).toContain('누적 처리 0건 실패 7건');
  });

  it('여러 컨슈머의 건수를 합쳐 머리줄에 보여준다', () => {
    const lines = formatReport([
      { stats: buildStats({ processed: 100, failed: 1 }), crashed: false },
      {
        stats: buildStats({ consumerId: 'c-2', processed: 50, failed: 2 }),
        crashed: false,
      },
    ]);

    expect(lines[0]).toContain('누적 처리 150건 실패 3건');
    // 머리줄 하나 + 컨슈머 두 줄
    expect(lines).toHaveLength(3);
  });
});

/**
 * 지연 지표끼리 앞뒤가 맞아야 한다.
 *
 * 평균이 p95 보다 크게 나오면 둘 중 하나는 틀린 것이다. 실제로 1ms 해상도
 * 시계(Date.now)로 재던 시절, p95 가 0.000ms 인데 평균이 0.043ms 로 찍혔다.
 * 1ms 보다 짧은 처리가 전부 0 으로 뭉개져서 생긴 일이다.
 */
describe('지연 지표의 앞뒤', () => {
  it('평균이 p95 를 넘지 않는다', () => {
    const stats = buildStats({
      processed: 100,
      // 마이크로초 단위 값. 1ms 해상도로 재면 전부 0 이 된다.
      latenciesMs: Array.from({ length: 100 }, (_, i) => 0.01 + i * 0.001),
    });

    const line = formatConsumerLine(stats, false);
    const avg = Number(/평균=([\d.]+)ms/.exec(line)?.[1]);
    const p95 = Number(/p95=([\d.]+)ms/.exec(line)?.[1]);

    expect(Number.isFinite(avg)).toBe(true);
    expect(Number.isFinite(p95)).toBe(true);
    expect(avg).toBeLessThanOrEqual(p95);
  });

  it('1ms 보다 짧은 처리도 0 으로 뭉개지 않는다', () => {
    const stats = buildStats({
      processed: 3,
      latenciesMs: [0.012, 0.015, 0.018],
    });

    const line = formatConsumerLine(stats, false);

    // 0.000ms 로 찍히면 해상도를 잃은 것이다.
    expect(line).not.toContain('평균=0.000ms');
    expect(line).toContain('평균=0.015ms');
  });
});
