import { validateEnv } from './env.validation';

describe('validateEnv', () => {
  it('유효한 값은 통과해야 한다', () => {
    expect(() =>
      validateEnv({
        PORT: '3000',
        QUEUE_CONCURRENT_TASKS: '50',
        QUEUE_EXECUTION_TIMEOUT_MS: '10000',
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
});
