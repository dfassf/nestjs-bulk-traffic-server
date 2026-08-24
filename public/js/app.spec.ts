import * as fs from 'fs';
import * as path from 'path';
import * as vm from 'vm';

/**
 * 탭 전환 검증.
 *
 * app.js 는 파일 하단에서 초기화 함수를 바로 호출하는 브라우저 스크립트라
 * 통째로 평가하면 차트 생성·서버 조회 같은 부작용이 따라온다.
 * 함수 정의부만 잘라서 평가하고, 필요한 전역은 가짜로 채운다.
 */
interface FakeElement {
  dataset: Record<string, string>;
  id: string;
  classes: Set<string>;
  classList: { add(c: string): void; remove(c: string): void };
}

function createElement(id: string, tabName?: string): FakeElement {
  const classes = new Set<string>();
  return {
    id,
    dataset: tabName ? { tab: tabName } : {},
    classes,
    classList: {
      add: (c: string) => classes.add(c),
      remove: (c: string) => classes.delete(c),
    },
  };
}

function loadSwitchTab(tabs: string[]) {
  const source = fs.readFileSync(path.resolve(__dirname, 'app.js'), 'utf8');
  // 초기화 호출 직전까지만 평가한다. 그 뒤는 부작용 구간이다.
  const definitionsOnly = source.split('// 초기화')[0];

  const buttons = tabs.map((name) => createElement(`btn-${name}`, name));
  const panels = tabs.map((name) => createElement(`panel-${name}`));
  const calls = { start: 0, stop: 0 };

  const document = {
    querySelectorAll: (selector: string) => {
      if (selector === '.tab-btn') return buttons;
      if (selector === '.tab-panel') return panels;
      return [];
    },
    querySelector: (selector: string) => {
      const match = /\.tab-btn\[data-tab="(.+)"\]/.exec(selector);
      if (!match) return null;
      return buttons.find((b) => b.dataset.tab === match[1]) ?? null;
    },
    getElementById: (id: string) => panels.find((p) => p.id === id) ?? null,
  };

  const context: Record<string, unknown> = {
    document,
    kfStartPolling: () => calls.start++,
    kfStopPolling: () => calls.stop++,
    API_BASE: '',
    fetch: () => Promise.reject(new Error('테스트에서 서버를 부르지 않습니다')),
    setInterval: () => 0,
  };
  vm.createContext(context);
  vm.runInContext(definitionsOnly, context);

  return {
    switchTab: context.switchTab as (tab: string) => void,
    bindTabs: context.bindTabs as () => void,
    buttons,
    panels,
    calls,
  };
}

const TABS = ['perf', 'compare', 'kafka'];

describe('switchTab', () => {
  it('선택한 탭의 버튼과 패널만 활성화한다', () => {
    const { switchTab, buttons, panels } = loadSwitchTab(TABS);

    switchTab('compare');

    expect(buttons.find((b) => b.dataset.tab === 'compare')!.classes.has('active')).toBe(true);
    expect(buttons.find((b) => b.dataset.tab === 'perf')!.classes.has('active')).toBe(false);
    expect(panels.find((p) => p.id === 'panel-compare')!.classes.has('active')).toBe(true);
    expect(panels.find((p) => p.id === 'panel-perf')!.classes.has('active')).toBe(false);
  });

  it('탭을 바꾸면 이전 탭이 꺼진다', () => {
    const { switchTab, panels } = loadSwitchTab(TABS);

    switchTab('kafka');
    switchTab('perf');

    expect(panels.find((p) => p.id === 'panel-kafka')!.classes.has('active')).toBe(false);
    expect(panels.find((p) => p.id === 'panel-perf')!.classes.has('active')).toBe(true);
  });

  // 카프카 탭은 2초마다 서버를 조회한다. 다른 탭에서도 계속 조회하면
  // 실험 중인 서버에 불필요한 요청이 가서 측정값이 흔들린다.
  it('카프카 탭으로 가면 조회를 시작한다', () => {
    const { switchTab, calls } = loadSwitchTab(TABS);

    switchTab('kafka');

    expect(calls.start).toBe(1);
    expect(calls.stop).toBe(0);
  });

  it('카프카 탭을 벗어나면 조회를 멈춘다', () => {
    const { switchTab, calls } = loadSwitchTab(TABS);

    switchTab('kafka');
    switchTab('perf');

    expect(calls.start).toBe(1);
    expect(calls.stop).toBe(1);
  });

  it.each(['perf', 'compare'])('%s 탭에서는 조회하지 않는다', (tab) => {
    const { switchTab, calls } = loadSwitchTab(TABS);

    switchTab(tab);

    expect(calls.start).toBe(0);
    expect(calls.stop).toBe(1);
  });

  // 버튼만 있고 패널이 없으면 화면이 빈 채로 남는다. 그 상태를 조용히 넘기지 않는다.
  it('없는 탭으로 바꾸려 하면 예외를 던진다', () => {
    const { switchTab } = loadSwitchTab(TABS);

    expect(() => switchTab('없는탭')).toThrow(/그런 탭이 없습니다/);
  });

  it('패널이 빠진 탭도 예외로 알린다', () => {
    const { switchTab } = loadSwitchTab(['perf', 'orphan']);
    // 패널 목록에서 orphan 을 지워 짝이 안 맞는 상황을 만든다.
    expect(() => switchTab('없는것')).toThrow();
  });
});

describe('bindTabs', () => {
  it('모든 탭 버튼을 전환 함수에 연결한다', () => {
    const source = fs.readFileSync(path.resolve(__dirname, 'app.js'), 'utf8');
    const definitionsOnly = source.split('// 초기화')[0];

    const listeners: { tab: string; handler: () => void }[] = [];
    const buttons = TABS.map((name) => ({
      dataset: { tab: name },
      classList: { add: () => undefined, remove: () => undefined },
      addEventListener: (_event: string, handler: () => void) => {
        listeners.push({ tab: name, handler });
      },
    }));

    const context: Record<string, unknown> = {
      document: {
        querySelectorAll: (selector: string) => (selector === '.tab-btn' ? buttons : []),
        querySelector: () => null,
        getElementById: () => null,
      },
      kfStartPolling: () => undefined,
      kfStopPolling: () => undefined,
      API_BASE: '',
      fetch: () => Promise.reject(new Error('사용 안 함')),
      setInterval: () => 0,
    };
    vm.createContext(context);
    vm.runInContext(definitionsOnly, context);

    (context.bindTabs as () => void)();

    expect(listeners.map((l) => l.tab)).toEqual(TABS);
  });
});
