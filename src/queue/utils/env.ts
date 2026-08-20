export function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/** 워커 엔진 선택값. 각 값의 의미는 README 의 워커 엔진 절 참고. */
export const WORKER_ENGINES = ['node', 'go', 'both', 'kafka'] as const;

export type WorkerEngine = (typeof WORKER_ENGINES)[number];

export const DEFAULT_WORKER_ENGINE: WorkerEngine = 'node';

/**
 * WORKER_ENGINE 환경변수를 워커 엔진 값으로 해석한다.
 *
 * 이 함수가 해석의 단일 창구다. 예전에는 EngineRouterService 와
 * KafkaProducerBackend 가 각자 비교했고, 한쪽만 소문자로 바꾸는 바람에
 * WORKER_ENGINE=Kafka 로 주면 라우터는 kafka 모드인데 프로듀서는
 * 연결하지 않는 어긋남이 있었다.
 *
 * 알 수 없는 값은 기본값으로 떨어뜨리지 않고 null 을 돌려준다.
 * 오타를 조용히 삼키지 않기 위해서이며, 부팅 시 검증은 validateEnv 가 맡는다.
 */
export function parseWorkerEngine(raw: string | undefined): WorkerEngine | null {
  if (raw === undefined || raw === null) return null;

  const normalized = String(raw).trim().toLowerCase();
  if (normalized === '') return null;

  return (WORKER_ENGINES as readonly string[]).includes(normalized)
    ? (normalized as WorkerEngine)
    : null;
}

/**
 * 현재 프로세스의 워커 엔진. 미설정이면 기본값(node).
 * 알 수 없는 값은 validateEnv 가 부팅 시점에 막으므로 여기서는 기본값으로 처리한다.
 */
export function readWorkerEngineEnv(): WorkerEngine {
  return parseWorkerEngine(process.env.WORKER_ENGINE) ?? DEFAULT_WORKER_ENGINE;
}

/** KAFKA_BROKERS 를 브로커 주소 목록으로 해석한다. 콤마로 구분. */
export function readKafkaBrokersEnv(fallback = 'localhost:9092'): string[] {
  return (process.env.KAFKA_BROKERS ?? fallback)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}
