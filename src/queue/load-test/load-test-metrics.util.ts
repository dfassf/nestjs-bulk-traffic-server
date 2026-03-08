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
  avgMs: number;
  p50: number;
  p95: number;
  p99: number;
  minMs: number;
  maxMs: number;
}

export function computeMetrics(
  results: TaskResult[],
  totalMs: number,
): StreamMetrics {
  const fulfilled = results.filter((r) => r.ok);
  const rejected = results.filter((r) => !r.ok);
  const durations = fulfilled.map((r) => r.ms).sort((a, b) => a - b);

  return {
    total: results.length,
    fulfilled: fulfilled.length,
    rejected: rejected.length,
    totalMs,
    avgMs:
      durations.length > 0
        ? Math.round(
            (durations.reduce((sum, v) => sum + v, 0) / durations.length) * 100,
          ) / 100
        : 0,
    p50: durations[Math.floor(durations.length * 0.5)] ?? 0,
    p95: durations[Math.floor(durations.length * 0.95)] ?? 0,
    p99: durations[Math.floor(durations.length * 0.99)] ?? 0,
    minMs: durations[0] ?? 0,
    maxMs: durations[durations.length - 1] ?? 0,
  };
}
