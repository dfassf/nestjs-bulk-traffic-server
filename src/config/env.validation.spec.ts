import { validateEnv } from './env.validation';

describe('validateEnv', () => {
  it('유효한 값은 통과해야 한다', () => {
    expect(() =>
      validateEnv({
        PORT: '3000',
        QUEUE_CONCURRENT_TASKS: '50',
        QUEUE_EXECUTION_TIMEOUT_MS: '10000',
        QUEUE_SNAPSHOT_INTERVAL_MS: '30000',
        QUEUE_SNAPSHOT_MAX_AGE_MS: '300000',
        QUEUE_PERSISTENCE: 'file',
        QUEUE_SNAPSHOT_PATH: '.queue-snapshot.json',
        WORKER_POOL_SIZE: '4',
        WORKER_MAX_CPU_CONCURRENCY: '4',
        WORKER_MAX_MEMORY_CONCURRENCY: '2',
        WORKER_MAX_CUSTOM_CONCURRENCY: '2',
      }),
    ).not.toThrow();
  });

  it('양의 정수가 아닌 값은 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_MAX_MEMORY_CONCURRENCY: '0',
      }),
    ).toThrow('WORKER_MAX_MEMORY_CONCURRENCY는 0보다 큰 정수여야 합니다.');
  });

  it('ALLOWED_ORIGINS가 문자열이 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        ALLOWED_ORIGINS: ['http://localhost:3000'],
      }),
    ).toThrow('ALLOWED_ORIGINS는 콤마(,)로 구분된 문자열이어야 합니다.');
  });

  it('DISABLE_WORKERS 값이 true/false가 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        DISABLE_WORKERS: '1',
      }),
    ).toThrow('DISABLE_WORKERS는 true 또는 false 문자열이어야 합니다.');
  });

  it('QUEUE_PERSISTENCE 값이 file/none이 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        QUEUE_PERSISTENCE: 'redis',
      }),
    ).toThrow('QUEUE_PERSISTENCE는 file 또는 none 문자열이어야 합니다.');
  });

  it('QUEUE_SNAPSHOT_PATH가 문자열이 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        QUEUE_SNAPSHOT_PATH: 1234,
      }),
    ).toThrow('QUEUE_SNAPSHOT_PATH는 문자열이어야 합니다.');
  });
});
