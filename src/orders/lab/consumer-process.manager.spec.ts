import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ConsumerProcessManager } from './consumer-process.manager';

/**
 * 실제로 자식 프로세스를 띄워서 검증한다.
 *
 * spawn 을 목킹하면 "환경변수를 제대로 넘겼는지" 는 봐도
 * "정말 프로세스가 뜨고 죽는지" 는 확인하지 못한다.
 * 짧게 살다 죽는 스크립트를 만들어 실제 동작을 본다.
 */
describe('ConsumerProcessManager', () => {
  let tmpDir: string;
  let scriptPath: string;
  let manager: ConsumerProcessManager;

  /** 환경변수를 찍고 신호를 기다리는 가짜 컨슈머. */
  function writeScript(body: string): string {
    const file = path.join(
      tmpDir,
      `script-${Math.random().toString(36).slice(2)}.js`,
    );
    fs.writeFileSync(file, body);
    return file;
  }

  function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
    return new Promise((resolve, reject) => {
      const startedAt = Date.now();
      const timer = setInterval(() => {
        if (check()) {
          clearInterval(timer);
          resolve();
        } else if (Date.now() - startedAt > timeoutMs) {
          clearInterval(timer);
          reject(new Error('조건이 시간 안에 만족되지 않았습니다.'));
        }
      }, 50);
    });
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lab-proc-'));
    scriptPath = writeScript(
      `console.log('그룹=' + process.env.CONSUMER_GROUP_ID);
       console.log('개수=' + process.env.CONSUMER_COUNT);
       console.log('커밋=' + process.env.CONSUMER_COMMIT_MODE);
       console.log('세션만료=' + process.env.CONSUMER_SESSION_TIMEOUT_MS);
       setInterval(() => {}, 1000);`,
    );
    manager = new ConsumerProcessManager(scriptPath);
  });

  afterEach(async () => {
    manager.stopAll('SIGKILL');
    await new Promise((resolve) => setTimeout(resolve, 200));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  describe('설정 검증', () => {
    // 잘못된 값을 기본값으로 흡수하면 어떤 설정으로 실험 중인지 모르게 된다.
    it.each([
      [{ instances: 0 }, /1~20 사이/],
      [{ instances: 21 }, /1~20 사이/],
      [{ instances: 1.5 }, /1~20 사이/],
      [{ processingDelayMs: -1 }, /0~60000ms/],
      [{ processingDelayMs: 60001 }, /0~60000ms/],
      [{ commitDelayMs: -1 }, /0~60000ms/],
      [{ crashAfter: -1 }, /0 이상 정수/],
      [{ commitMode: 'after' as any }, /after-process 또는 before-process/],
      [{ groupId: '   ' }, /비어 있습니다/],
      // 브로커가 받아주는 범위를 벗어나면 그룹 참여 자체를 거부당한다.
      [{ sessionTimeoutMs: 5999 }, /6000~1800000ms/],
      [{ sessionTimeoutMs: 1_800_001 }, /6000~1800000ms/],
      [{ sessionTimeoutMs: 10.5 }, /6000~1800000ms/],
    ])('잘못된 설정 %j 은 예외를 던진다', (options, pattern) => {
      expect(() => manager.spawnConsumer(options)).toThrow(pattern);
    });

    it('스크립트가 없으면 명확한 안내와 함께 실패한다', () => {
      const missing = new ConsumerProcessManager(
        path.join(tmpDir, '없는파일.js'),
      );
      expect(() => missing.spawnConsumer()).toThrow(/npm run build/);
    });
  });

  describe('프로세스 실행', () => {
    it('설정을 환경변수로 넘긴다', async () => {
      const info = manager.spawnConsumer({
        groupId: 'lab-group',
        instances: 3,
        commitMode: 'before-process',
        sessionTimeoutMs: 10_000,
      });

      await waitFor(() => info.recentLogs.length >= 4);

      expect(info.recentLogs).toContain('그룹=lab-group');
      expect(info.recentLogs).toContain('개수=3');
      expect(info.recentLogs).toContain('커밋=before-process');
      // 이 값이 안 넘어가면 조작판에서 뭘 바꾸든 계곡 폭이 그대로다.
      expect(info.recentLogs).toContain('세션만료=10000');
    });

    it('세션 만료 시간을 안 주면 기본 60초로 넘긴다', async () => {
      const info = manager.spawnConsumer({ groupId: 'lab-default' });

      await waitFor(() => info.recentLogs.length >= 4);

      expect(info.recentLogs).toContain('세션만료=60000');
    });

    it('띄운 프로세스를 목록에 담는다', async () => {
      const info = manager.spawnConsumer({ groupId: 'g1' });

      const list = manager.list();
      expect(list).toHaveLength(1);
      expect(list[0].pid).toBe(info.pid);
      expect(list[0].alive).toBe(true);
    });

    it('여러 개를 동시에 띄운다', () => {
      manager.spawnConsumer({ groupId: 'g1' });
      manager.spawnConsumer({ groupId: 'g2' });

      expect(manager.list()).toHaveLength(2);
    });
  });

  describe('종료', () => {
    // SIGKILL 은 정리할 틈 없이 즉시 죽는다. 오프셋 커밋도 못 하므로
    // 재시작하면 중복 처리가 관측된다. 실무에서 중복이 생기는 상황이 이것이다.
    it('SIGKILL 로 즉시 죽인다', async () => {
      const info = manager.spawnConsumer({ groupId: 'g1' });

      manager.stop(info.pid, 'SIGKILL');
      await waitFor(() => !info.alive);

      expect(info.alive).toBe(false);
      expect(info.exitSignal).toBe('SIGKILL');
    });

    // SIGTERM 은 정상 종료다. 컨슈머는 이 신호를 받으면 오프셋을 커밋하고 빠지므로
    // 중복이 생기지 않는다. 중복 실험에 SIGKILL 을 쓰는 이유가 이 차이다.
    it('SIGTERM 으로 정상 종료한다', async () => {
      const gentle = writeScript(
        `process.on('SIGTERM', () => process.exit(0));
         console.log('준비됨');
         setInterval(() => {}, 1000);`,
      );
      const gentleManager = new ConsumerProcessManager(gentle);
      const info = gentleManager.spawnConsumer({ groupId: 'g1' });

      // 핸들러가 등록되기 전에 신호가 가면 기본 동작(즉시 종료)이 실행되어
      // 정상 종료인지 강제 종료인지 구분할 수 없다. 준비를 기다린다.
      await waitFor(() => info.recentLogs.includes('준비됨'));
      gentleManager.stop(info.pid, 'SIGTERM');
      await waitFor(() => !info.alive);

      expect(info.exitCode).toBe(0);
      expect(info.exitSignal).toBeNull();
    });

    it('없는 pid 는 예외를 던진다', () => {
      expect(() => manager.stop(999999)).toThrow(
        /그런 컨슈머 프로세스가 없습니다/,
      );
    });

    // 이미 죽은 pid 를 재사용해 다른 프로세스를 죽이면 안 된다.
    it('이미 종료된 프로세스는 다시 죽이지 않는다', async () => {
      const info = manager.spawnConsumer({ groupId: 'g1' });
      manager.stop(info.pid, 'SIGKILL');
      await waitFor(() => !info.alive);

      expect(() => manager.stop(info.pid)).toThrow(/이미 종료된/);
    });

    it('전체 종료는 살아있는 개수를 돌려준다', async () => {
      manager.spawnConsumer({ groupId: 'g1' });
      manager.spawnConsumer({ groupId: 'g2' });

      expect(manager.stopAll('SIGKILL')).toBe(2);
      await waitFor(() => manager.list().every((p) => !p.alive));
    });
  });

  describe('목록 정리', () => {
    it('종료된 기록만 지운다', async () => {
      const dead = manager.spawnConsumer({ groupId: 'dead' });
      manager.spawnConsumer({ groupId: 'alive' });

      manager.stop(dead.pid, 'SIGKILL');
      await waitFor(() => !dead.alive);

      expect(manager.clearFinished()).toBe(1);
      const remaining = manager.list();
      expect(remaining).toHaveLength(1);
      expect(remaining[0].groupId).toBe('alive');
    });
  });
});
