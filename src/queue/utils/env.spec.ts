import {
  DEFAULT_WORKER_ENGINE,
  parseWorkerEngine,
  readKafkaBrokersEnv,
  readPositiveIntEnv,
  readWorkerEngineEnv,
} from './env';

describe('readPositiveIntEnv', () => {
  afterEach(() => {
    delete process.env.TEST_INT_VALUE;
  });

  it('미설정이면 기본값을 돌려준다', () => {
    expect(readPositiveIntEnv('TEST_INT_VALUE', 7)).toBe(7);
  });

  it('양의 정수면 그 값을 돌려준다', () => {
    process.env.TEST_INT_VALUE = '42';
    expect(readPositiveIntEnv('TEST_INT_VALUE', 7)).toBe(42);
  });

  it('0 이하거나 숫자가 아니면 기본값을 돌려준다', () => {
    process.env.TEST_INT_VALUE = '0';
    expect(readPositiveIntEnv('TEST_INT_VALUE', 7)).toBe(7);

    process.env.TEST_INT_VALUE = 'abc';
    expect(readPositiveIntEnv('TEST_INT_VALUE', 7)).toBe(7);
  });
});

describe('parseWorkerEngine', () => {
  it.each(['node', 'go', 'both', 'kafka'])('%s 는 그대로 해석한다', (value) => {
    expect(parseWorkerEngine(value)).toBe(value);
  });

  // 이 케이스가 무너지면서 EngineRouterService 와 KafkaProducerBackend 의
  // 판정이 갈렸다. 대소문자·공백을 흡수하는지 고정해 둔다.
  it.each([
    ['Kafka', 'kafka'],
    ['KAFKA', 'kafka'],
    [' kafka ', 'kafka'],
    ['Go', 'go'],
    ['BOTH', 'both'],
  ])('%s 는 %s 로 정규화한다', (raw, expected) => {
    expect(parseWorkerEngine(raw)).toBe(expected);
  });

  it('알 수 없는 값은 기본값으로 떨어뜨리지 않고 null 을 돌려준다', () => {
    expect(parseWorkerEngine('kfka')).toBeNull();
    expect(parseWorkerEngine('redis')).toBeNull();
  });

  it('미설정·빈 문자열은 null 을 돌려준다', () => {
    expect(parseWorkerEngine(undefined)).toBeNull();
    expect(parseWorkerEngine('')).toBeNull();
    expect(parseWorkerEngine('   ')).toBeNull();
  });
});

describe('readWorkerEngineEnv', () => {
  afterEach(() => {
    delete process.env.WORKER_ENGINE;
  });

  it('미설정이면 기본값을 돌려준다', () => {
    expect(readWorkerEngineEnv()).toBe(DEFAULT_WORKER_ENGINE);
  });

  it('대소문자가 섞여도 같은 엔진으로 해석한다', () => {
    process.env.WORKER_ENGINE = 'Kafka';
    expect(readWorkerEngineEnv()).toBe('kafka');
  });

  it('알 수 없는 값은 기본값으로 처리한다 (부팅 차단은 validateEnv 담당)', () => {
    process.env.WORKER_ENGINE = 'kfka';
    expect(readWorkerEngineEnv()).toBe(DEFAULT_WORKER_ENGINE);
  });
});

describe('readKafkaBrokersEnv', () => {
  afterEach(() => {
    delete process.env.KAFKA_BROKERS;
  });

  it('미설정이면 기본 브로커를 돌려준다', () => {
    expect(readKafkaBrokersEnv()).toEqual(['localhost:9092']);
  });

  it('콤마로 구분한 목록을 공백 제거해서 돌려준다', () => {
    process.env.KAFKA_BROKERS = ' a:9092 , b:9092 ';
    expect(readKafkaBrokersEnv()).toEqual(['a:9092', 'b:9092']);
  });

  it('빈 항목은 걸러낸다', () => {
    process.env.KAFKA_BROKERS = 'a:9092,,b:9092,';
    expect(readKafkaBrokersEnv()).toEqual(['a:9092', 'b:9092']);
  });
});
