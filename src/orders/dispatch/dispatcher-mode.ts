/**
 * 주문 이벤트를 무엇으로 내보낼지.
 *
 *   kafka   카프카로 발행. 별도 프로세스가 소비한다.
 *   memory  이 프로세스 안의 큐에 쌓고 같은 프로세스가 소비한다.
 *
 * 둘을 나란히 재기 위한 스위치다. 실험 11(인메모리 큐 vs 카프카)에서 쓴다.
 */
export const DISPATCHER_MODES = ['kafka', 'memory'] as const;

export type DispatcherMode = (typeof DISPATCHER_MODES)[number];

/**
 * ORDER_DISPATCHER 환경변수를 해석한다.
 *
 * 값이 없으면 카프카다. 이 프로젝트의 기본 흐름이기 때문이다.
 * 목록에 없는 값은 기본값으로 떨어뜨리지 않고 에러를 낸다.
 * 오타 하나로 엉뚱한 수단으로 측정하면 그 숫자를 신뢰할 수 없다.
 */
export function readDispatcherModeEnv(): DispatcherMode {
  const raw = process.env.ORDER_DISPATCHER;
  if (raw === undefined || raw.trim() === '') return 'kafka';

  const normalized = raw.trim().toLowerCase();
  if ((DISPATCHER_MODES as readonly string[]).includes(normalized)) {
    return normalized as DispatcherMode;
  }

  throw new Error(
    `ORDER_DISPATCHER 는 ${DISPATCHER_MODES.join(' 또는 ')} 여야 합니다. 현재 값: ${raw}`,
  );
}
