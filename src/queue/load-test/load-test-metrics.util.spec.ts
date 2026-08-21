import { computeMetrics, TaskResult } from './load-test-metrics.util';

function ok(index: number, ms: number): TaskResult {
  return { index, ok: true, ms };
}

function fail(index: number): TaskResult {
  return { index, ok: false, ms: 0 };
}

describe('computeMetrics', () => {
  it('성공 건들의 지연 분포를 계산한다', () => {
    const results = [ok(0, 10), ok(1, 20), ok(2, 30), ok(3, 40)];
    const metrics = computeMetrics(results, 100);

    expect(metrics.total).toBe(4);
    expect(metrics.fulfilled).toBe(4);
    expect(metrics.rejected).toBe(0);
    expect(metrics.avgMs).toBe(25);
    expect(metrics.minMs).toBe(10);
    expect(metrics.maxMs).toBe(40);
  });

  it('실패 건은 지연 분포에서 제외한다', () => {
    const results = [ok(0, 10), fail(1), ok(2, 30)];
    const metrics = computeMetrics(results, 100);

    expect(metrics.total).toBe(3);
    expect(metrics.fulfilled).toBe(2);
    expect(metrics.rejected).toBe(1);
    expect(metrics.avgMs).toBe(20);
    expect(metrics.minMs).toBe(10);
    expect(metrics.maxMs).toBe(30);
  });

  // 표본이 없으면 "지연 0ms"가 아니라 "측정 불가"다.
  // 0으로 메우면 대시보드에 가장 좋은 수치로 표시되어 문제를 숨긴다.
  it('성공 건이 없으면 지연 지표는 null 이다', () => {
    const metrics = computeMetrics([fail(0), fail(1)], 100);

    expect(metrics.total).toBe(2);
    expect(metrics.fulfilled).toBe(0);
    expect(metrics.rejected).toBe(2);
    expect(metrics.avgMs).toBeNull();
    expect(metrics.p50).toBeNull();
    expect(metrics.p95).toBeNull();
    expect(metrics.p99).toBeNull();
    expect(metrics.minMs).toBeNull();
    expect(metrics.maxMs).toBeNull();
  });

  it('결과가 아예 없어도 지연 지표는 null 이다', () => {
    const metrics = computeMetrics([], 0);

    expect(metrics.total).toBe(0);
    expect(metrics.p99).toBeNull();
    expect(metrics.minMs).toBeNull();
  });

  it('성공 건이 하나면 모든 분위수가 그 값이다', () => {
    const metrics = computeMetrics([ok(0, 42)], 50);

    expect(metrics.avgMs).toBe(42);
    expect(metrics.p50).toBe(42);
    expect(metrics.p95).toBe(42);
    expect(metrics.p99).toBe(42);
    expect(metrics.minMs).toBe(42);
    expect(metrics.maxMs).toBe(42);
  });

  it('분위수는 정렬된 표본에서 고른다', () => {
    const results = Array.from({ length: 100 }, (_, i) => ok(i, 100 - i));
    const metrics = computeMetrics(results, 1000);

    expect(metrics.minMs).toBe(1);
    expect(metrics.maxMs).toBe(100);
    expect(metrics.p50).toBe(51);
    expect(metrics.p99).toBe(100);
  });
});
