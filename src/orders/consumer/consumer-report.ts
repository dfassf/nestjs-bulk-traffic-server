import {
  ConsumerPerformance,
  ConsumerStats,
  summarizePerformance,
} from './order-consumer';

/**
 * 컨슈머 현황을 화면에 보여줄 문자열로 만든다.
 *
 * 실행 진입점(consumer.ts)에서 분리했다. 그쪽은 최상위에서 카프카에 붙어버려
 * import 만 해도 접속을 시도하므로 테스트할 수가 없다.
 * 여기 있는 것들은 값을 받아 문자열만 돌려주는 순수 함수라 그대로 검증된다.
 */

/**
 * 측정 못 한 값은 0 이 아니라 '측정 불가'로 보여준다.
 *
 * 0ms 는 가장 좋아 보이는 값이라, 한 건도 처리 못 한 결과가
 * 가장 빠른 것처럼 읽힌다. Go 컨슈머의 FormatDuration 과 같은 규칙이다.
 */
export function formatMs(value: number | null): string {
  return value === null ? '측정 불가' : `${value.toFixed(3)}ms`;
}

export function formatThroughput(value: number | null): string {
  return value === null ? '측정 불가' : `${value.toFixed(1)}건/초`;
}

/**
 * 파티션별 처리 건수. 하나도 없으면 이 컨슈머가 놀고 있다는 뜻이다
 * (파티션보다 컨슈머가 많을 때).
 */
export function formatPartitions(counts: Record<number, number>): string {
  const entries = Object.entries(counts).sort(
    ([a], [b]) => Number(a) - Number(b),
  );
  if (entries.length === 0) return '(할당된 파티션 없음)';

  return entries
    .map(([partition, count]) => `p${partition}:${count}`)
    .join(' ');
}

/** 컨슈머 한 줄. crashed 는 커밋 없이 빠졌는지 여부다. */
export function formatConsumerLine(
  stats: ConsumerStats,
  crashed: boolean,
  perf: ConsumerPerformance = summarizePerformance(stats),
): string {
  // 빠진 컨슈머는 표시해준다. 안 그러면 처리량이 왜 줄었는지 읽을 수 없다.
  const state = crashed ? ' [커밋 없이 빠짐]' : '';

  return (
    `  ${stats.consumerId} 처리=${stats.processed} ` +
    `${formatPartitions(stats.partitionCounts)} ` +
    `평균=${formatMs(perf.avgMs)} p95=${formatMs(perf.p95Ms)} ` +
    `${formatThroughput(perf.throughputPerSec)}${state}`
  );
}

/**
 * 전체 현황. 아직 아무 일도 없었으면 빈 배열을 돌려준다.
 *
 * 처리도 실패도 0 이면 보여줄 게 없다. 매 주기마다 "0건" 을 찍으면
 * 정작 봐야 할 줄이 묻힌다.
 */
export function formatReport(
  entries: { stats: ConsumerStats; crashed: boolean }[],
): string[] {
  const total = entries.reduce((sum, entry) => sum + entry.stats.processed, 0);
  const failed = entries.reduce((sum, entry) => sum + entry.stats.failed, 0);
  if (total === 0 && failed === 0) return [];

  return [
    `[consumer] 누적 처리 ${total}건 실패 ${failed}건`,
    ...entries.map((entry) => formatConsumerLine(entry.stats, entry.crashed)),
  ];
}
