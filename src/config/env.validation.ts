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

  const allowedOrigins = config.ALLOWED_ORIGINS;
  if (allowedOrigins !== undefined && typeof allowedOrigins !== 'string') {
    throw new Error('ALLOWED_ORIGINS는 콤마(,)로 구분된 문자열이어야 합니다.');
  }

  return config;
}
