import { validateEnv } from './env.validation';

describe('validateEnv', () => {
  it('유효한 값은 통과해야 한다', () => {
    expect(() =>
      validateEnv({
        PORT: '3000',
        QUEUE_CONCURRENT_TASKS: '50',
        QUEUE_EXECUTION_TIMEOUT_MS: '10000',
      }),
    ).not.toThrow();
  });

  it('양의 정수가 아닌 값은 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        QUEUE_CONCURRENT_TASKS: '0',
      }),
    ).toThrow('QUEUE_CONCURRENT_TASKS는 0보다 큰 정수여야 합니다.');
  });

  it('ALLOWED_ORIGINS가 문자열이 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        ALLOWED_ORIGINS: ['http://localhost:3000'],
      }),
    ).toThrow('ALLOWED_ORIGINS는 콤마(,)로 구분된 문자열이어야 합니다.');
  });
});
