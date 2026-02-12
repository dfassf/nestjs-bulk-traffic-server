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

  return config;
}
