import { parseWorkerEngine, WORKER_ENGINES } from '../queue/utils/env';

interface EnvMap {
  [key: string]: unknown;
}

const POSITIVE_INT_KEYS = [
  'PORT',
  'UV_THREADPOOL_SIZE',
  'QUEUE_CONCURRENT_TASKS',
  'QUEUE_MAX_CONCURRENT_REQUESTS',
  'QUEUE_TASK_TIMEOUT_MS',
  'QUEUE_EXECUTION_TIMEOUT_MS',
  'QUEUE_OVERFLOW_THRESHOLD',
  'QUEUE_PROCESS_INTERVAL_MS',
  'QUEUE_MEMORY_CHECK_INTERVAL_MS',
  'QUEUE_STATS_LOG_INTERVAL_MS',
  'QUEUE_SNAPSHOT_INTERVAL_MS',
  'QUEUE_SNAPSHOT_MAX_AGE_MS',
  'WORKER_POOL_SIZE',
  'WORKER_MAX_CPU_CONCURRENCY',
  'WORKER_MAX_MEMORY_CONCURRENCY',
  'WORKER_MAX_CUSTOM_CONCURRENCY',
];

function assertPositiveInt(value: unknown, key: string): void {
  if (value === undefined || value === null || value === '') {
    return;
  }

  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${key}는 0보다 큰 정수여야 합니다.`);
  }
}

export function validateEnv(config: EnvMap): EnvMap {
  for (const key of POSITIVE_INT_KEYS) {
    assertPositiveInt(config[key], key);
  }

  const disableWorkers = config.DISABLE_WORKERS;
  if (
    disableWorkers !== undefined &&
    disableWorkers !== 'true' &&
    disableWorkers !== 'false'
  ) {
    throw new Error('DISABLE_WORKERS는 true 또는 false 문자열이어야 합니다.');
  }

  const allowCustomWorkload = config.ALLOW_CUSTOM_WORKLOAD;
  if (
    allowCustomWorkload !== undefined &&
    allowCustomWorkload !== 'true' &&
    allowCustomWorkload !== 'false'
  ) {
    throw new Error(
      'ALLOW_CUSTOM_WORKLOAD는 true 또는 false 문자열이어야 합니다.',
    );
  }

  const queuePersistence = config.QUEUE_PERSISTENCE;
  if (
    queuePersistence !== undefined &&
    queuePersistence !== 'file' &&
    queuePersistence !== 'none'
  ) {
    throw new Error('QUEUE_PERSISTENCE는 file 또는 none 문자열이어야 합니다.');
  }

  const snapshotPath = config.QUEUE_SNAPSHOT_PATH;
  if (snapshotPath !== undefined && typeof snapshotPath !== 'string') {
    throw new Error('QUEUE_SNAPSHOT_PATH는 문자열이어야 합니다.');
  }

  const allowedOrigins = config.ALLOWED_ORIGINS;
  if (allowedOrigins !== undefined && typeof allowedOrigins !== 'string') {
    throw new Error('ALLOWED_ORIGINS는 콤마(,)로 구분된 문자열이어야 합니다.');
  }

  // 미설정(undefined)·빈 값('')·오타를 각각 다르게 다룬다.
  //   미설정 -> 기본값(node) 사용, 통과
  //   빈 값  -> 미설정과 같게 취급, 통과
  //   오타   -> 조용히 node 로 떨어뜨리지 않고 부팅에서 막는다
  const workerEngine = config.WORKER_ENGINE;
  const workerEngineSet =
    workerEngine !== undefined &&
    workerEngine !== null &&
    String(workerEngine).trim() !== '';

  if (workerEngineSet && parseWorkerEngine(String(workerEngine)) === null) {
    throw new Error(
      `WORKER_ENGINE은 ${WORKER_ENGINES.join(', ')} 중 하나여야 합니다.`,
    );
  }

  // 엔진이 kafka 인데 브로커 주소가 없으면 첫 발행에서야 실패한다. 부팅에서 막는다.
  if (workerEngineSet && parseWorkerEngine(String(workerEngine)) === 'kafka') {
    const brokers = config.KAFKA_BROKERS;
    if (brokers !== undefined && typeof brokers !== 'string') {
      throw new Error('KAFKA_BROKERS는 콤마(,)로 구분된 문자열이어야 합니다.');
    }
    // 미설정도 빈 값과 똑같이 막는다. kafka 모드에서 브로커 주소는 접속 대상이라
    // 기본값으로 대신하면 의도하지 않은 브로커에 붙을 수 있다.
    const parsedBrokers =
      typeof brokers === 'string'
        ? brokers
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean)
        : [];
    if (parsedBrokers.length === 0) {
      throw new Error(
        'WORKER_ENGINE=kafka 이면 KAFKA_BROKERS에 브로커 주소가 최소 하나 있어야 합니다.',
      );
    }
  }

  return config;
}
