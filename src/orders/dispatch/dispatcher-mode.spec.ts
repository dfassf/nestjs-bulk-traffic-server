import { readDispatcherModeEnv } from './dispatcher-mode';

describe('readDispatcherModeEnv', () => {
  afterEach(() => {
    delete process.env.ORDER_DISPATCHER;
  });

  it('값이 없으면 카프카다', () => {
    expect(readDispatcherModeEnv()).toBe('kafka');
  });

  it('빈 문자열도 카프카로 본다', () => {
    process.env.ORDER_DISPATCHER = '   ';

    expect(readDispatcherModeEnv()).toBe('kafka');
  });

  it.each([
    ['kafka', 'kafka'],
    ['memory', 'memory'],
    ['KAFKA', 'kafka'],
    [' Memory ', 'memory'],
  ])('%s 를 %s 로 읽는다', (raw, expected) => {
    process.env.ORDER_DISPATCHER = raw;

    expect(readDispatcherModeEnv()).toBe(expected);
  });

  // 오타를 기본값으로 흡수하면 엉뚱한 수단으로 측정하고도 모른다.
  // 둘을 비교하는 실험이라 어느 쪽으로 돌았는지가 결과의 전부다.
  it.each(['redis', 'kafkaa', 'in-memory', 'none'])(
    '목록에 없는 %s 는 에러다',
    (raw) => {
      process.env.ORDER_DISPATCHER = raw;

      expect(() => readDispatcherModeEnv()).toThrow(/kafka 또는 memory/);
    },
  );
});
