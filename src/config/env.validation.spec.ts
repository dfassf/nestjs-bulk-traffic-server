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
        ALLOW_CUSTOM_WORKLOAD: 'false',
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

  it('ALLOW_CUSTOM_WORKLOAD 값이 true/false가 아니면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        ALLOW_CUSTOM_WORKLOAD: '1',
      }),
    ).toThrow('ALLOW_CUSTOM_WORKLOAD는 true 또는 false 문자열이어야 합니다.');
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

  it.each(['node', 'go', 'both', 'kafka'])(
    'WORKER_ENGINE=%s 는 통과해야 한다',
    (engine) => {
      expect(() =>
        validateEnv({
          WORKER_ENGINE: engine,
          KAFKA_BROKERS: 'localhost:9092',
        }),
      ).not.toThrow();
    },
  );

  it('WORKER_ENGINE 대소문자·공백이 섞여도 통과해야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_ENGINE: ' Kafka ',
        KAFKA_BROKERS: 'localhost:9092',
      }),
    ).not.toThrow();
  });

  it('WORKER_ENGINE 오타는 조용히 넘기지 않고 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_ENGINE: 'kfka',
      }),
    ).toThrow('WORKER_ENGINE은 node, go, both, kafka 중 하나여야 합니다.');
  });

  it('WORKER_ENGINE 미설정은 기본값(node)이므로 통과해야 한다', () => {
    expect(() => validateEnv({})).not.toThrow();
  });

  it('WORKER_ENGINE=kafka 인데 KAFKA_BROKERS가 비면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_ENGINE: 'kafka',
        KAFKA_BROKERS: '  ,  ',
      }),
    ).toThrow(
      'WORKER_ENGINE=kafka 이면 KAFKA_BROKERS에 브로커 주소가 최소 하나 있어야 합니다.',
    );
  });

  // 미설정이 빈 값보다 느슨하면, 변수를 아예 빼먹었을 때 기본 브로커로 조용히 붙는다.
  it('WORKER_ENGINE=kafka 인데 KAFKA_BROKERS가 아예 없으면 예외를 던져야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_ENGINE: 'kafka',
      }),
    ).toThrow(
      'WORKER_ENGINE=kafka 이면 KAFKA_BROKERS에 브로커 주소가 최소 하나 있어야 합니다.',
    );
  });

  it('WORKER_ENGINE 이 빈 문자열이면 미설정과 같게 통과해야 한다', () => {
    expect(() => validateEnv({ WORKER_ENGINE: '' })).not.toThrow();
    expect(() => validateEnv({ WORKER_ENGINE: '   ' })).not.toThrow();
  });

  it('WORKER_ENGINE이 kafka가 아니면 KAFKA_BROKERS가 비어도 통과해야 한다', () => {
    expect(() =>
      validateEnv({
        WORKER_ENGINE: 'node',
        KAFKA_BROKERS: '',
      }),
    ).not.toThrow();
  });
});
