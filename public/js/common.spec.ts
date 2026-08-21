import * as fs from 'fs';
import * as path from 'path';
import * as vm from 'vm';

/**
 * 대시보드 공용 표시 함수 검증.
 *
 * common.js 는 브라우저에서 전역 스크립트로 읽히는 파일이라 export 가 없다.
 * 파일을 읽어 격리된 컨텍스트에서 평가한 뒤 전역 함수를 꺼내 쓴다.
 */
function loadCommonJs(): {
  formatMs: (v: unknown) => string;
  formatStatValue: (v: unknown, unit: string) => string;
} {
  const source = fs.readFileSync(
    path.resolve(__dirname, 'common.js'),
    'utf8',
  );
  // document·fetch 등 브라우저 전역은 이 테스트에서 쓰지 않는 함수에만 필요하다.
  const context: Record<string, unknown> = { document: undefined };
  vm.createContext(context);
  vm.runInContext(source, context);

  return {
    formatMs: context.formatMs as (v: unknown) => string,
    formatStatValue: context.formatStatValue as (v: unknown, unit: string) => string,
  };
}

describe('formatMs', () => {
  const { formatMs } = loadCommonJs();

  it('숫자는 ms 를 붙여 표시한다', () => {
    expect(formatMs(0)).toBe('0ms');
    expect(formatMs(42)).toBe('42ms');
    expect(formatMs(12.34)).toBe('12.34ms');
  });

  // 서버는 표본이 없으면 0 이 아니라 null 을 보낸다.
  // 화면에서 그대로 이어붙이면 "nullms" 가 되고, 0 으로 바꾸면
  // 가장 빠른 수치처럼 보인다. 둘 다 피해야 한다.
  it('null·undefined 는 측정 불가로 표시한다', () => {
    expect(formatMs(null)).toBe('측정 불가');
    expect(formatMs(undefined)).toBe('측정 불가');
  });

  it('실측 0ms 와 측정 불가를 구분한다', () => {
    expect(formatMs(0)).not.toBe(formatMs(null));
  });
});

describe('formatStatValue', () => {
  const { formatStatValue } = loadCommonJs();

  it('숫자는 값과 단위를 나눠 그린다', () => {
    expect(formatStatValue(15, 'ms')).toBe('15<span class="unit">ms</span>');
    expect(formatStatValue(3.5, 'req/s')).toBe('3.5<span class="unit">req/s</span>');
  });

  it('null·undefined 는 단위 없이 측정 불가만 보여준다', () => {
    expect(formatStatValue(null, 'ms')).toBe('<span class="unit">측정 불가</span>');
    expect(formatStatValue(undefined, 'ms')).toBe('<span class="unit">측정 불가</span>');
  });

  it('실측 0 은 측정 불가와 다르게 그린다', () => {
    expect(formatStatValue(0, 'ms')).toBe('0<span class="unit">ms</span>');
    expect(formatStatValue(0, 'ms')).not.toBe(formatStatValue(null, 'ms'));
  });
});
