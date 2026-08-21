export interface TaskResult {
  index: number;
  ok: boolean;
  ms: number;
}

export interface StreamMetrics {
  total: number;
  fulfilled: number;
  rejected: number;
  totalMs: number;
  /**
   * 지연 지표. 성공 건이 하나도 없으면 null(측정 불가)이다.
   *
   * 0 으로 메우지 않는다. 지연 0ms 는 가장 좋은 수치라서,
   * 전량 실패한 결과가 대시보드에서 완벽한 성능처럼 보이게 된다.
   */
  avgMs: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  minMs: number | null;
  maxMs: number | null;
}

/** 엔진 비교에서 한쪽 엔진의 지연 분포. 표본이 없으면 각 값이 null. */
export interface LatencySummary {
  avgMs: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  min: number | null;
  max: number | null;
}

/** 정렬된 표본에서 분위수를 고른다. 표본이 없으면 null. */
function percentile(sorted: number[], ratio: number): number | null {
  if (sorted.length === 0) return null;
  const index = Math.min(Math.floor(sorted.length * ratio), sorted.length - 1);
  return sorted[index];
}

/**
 * 지연 표본 목록에서 분포 요약을 만든다.
 *
 * computeMetrics 와 같은 규칙을 쓰되, 성공·실패 구분 없이 이미 걸러진
 * 표본만 받는 자리(엔진 비교)에서 사용한다. 표본이 없으면 0 이 아니라 null.
 */
export function summarizeLatencies(samples: number[]): LatencySummary {
  const sorted = [...samples].sort((a, b) => a - b);
  const hasSample = sorted.length > 0;

  return {
    avgMs: hasSample
      ? Math.round(
          (sorted.reduce((sum, v) => sum + v, 0) / sorted.length) * 100,
        ) / 100
      : null,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    min: hasSample ? sorted[0] : null,
    max: hasSample ? sorted[sorted.length - 1] : null,
  };
}

export function computeMetrics(
  results: TaskResult[],
  totalMs: number,
): StreamMetrics {
  const fulfilled = results.filter((r) => r.ok);
  const rejected = results.filter((r) => !r.ok);
  const durations = fulfilled.map((r) => r.ms).sort((a, b) => a - b);
  const hasSample = durations.length > 0;

  return {
    total: results.length,
    fulfilled: fulfilled.length,
    rejected: rejected.length,
    totalMs,
    avgMs: hasSample
      ? Math.round(
          (durations.reduce((sum, v) => sum + v, 0) / durations.length) * 100,
        ) / 100
      : null,
    p50: percentile(durations, 0.5),
    p95: percentile(durations, 0.95),
    p99: percentile(durations, 0.99),
    minMs: hasSample ? durations[0] : null,
    maxMs: hasSample ? durations[durations.length - 1] : null,
  };
}
