import { Worker } from 'worker_threads';
import * as path from 'path';

/**
 * 워커의 custom 실행기 오류 처리 검증.
 *
 * 실제 Worker 스레드를 띄워서 확인한다. 예전에는 실패를 { error } 로 정상 반환해서
 * 상위 집계가 success: true 로 세는 바람에, 전량 실패해도 통계가 100% 성공으로 보였다.
 */
function runCustomTask(
  message: Record<string, unknown>,
  env: Record<string, string>,
): Promise<{ success: boolean; error?: string; result?: unknown }> {
  const workerPath = path.resolve(__dirname, 'worker.js');

  return new Promise((resolve, reject) => {
    const worker = new Worker(workerPath, { env: { ...process.env, ...env } });
    const timer = setTimeout(() => {
      void worker.terminate();
      reject(new Error('워커 응답 시간 초과'));
    }, 10000);

    worker.on('message', (msg: any) => {
      // 기동 직후 오는 초기화·헬스체크 알림은 건너뛴다.
      if (msg?.initialized || msg?.healthCheck) return;
      clearTimeout(timer);
      void worker.terminate();
      resolve(msg);
    });

    worker.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    worker.once('online', () => worker.postMessage(message));
  });
}

describe('worker.js custom 실행기', () => {
  jest.setTimeout(20000);

  it('기능이 꺼져 있으면 실패로 보고한다', async () => {
    const res = await runCustomTask(
      { type: 'custom', operation: 'execute', params: {}, functionCode: 'return 1;' },
      { ALLOW_CUSTOM_WORKLOAD: 'false' },
    );

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/비활성화/);
  });

  it('함수 코드가 없으면 실패로 보고한다', async () => {
    const res = await runCustomTask(
      { type: 'custom', operation: 'execute', params: {} },
      { ALLOW_CUSTOM_WORKLOAD: 'true' },
    );

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/함수 코드/);
  });

  // 이 케이스가 핵심이다. 사용자 코드가 던진 예외를 성공으로 세면
  // totalProcessed 가 부풀고 대시보드가 100% 성공률을 보고한다.
  it('사용자 코드가 예외를 던지면 실패로 보고한다', async () => {
    const res = await runCustomTask(
      {
        type: 'custom',
        operation: 'execute',
        params: {},
        functionCode: 'throw new Error("의도한 실패");',
      },
      { ALLOW_CUSTOM_WORKLOAD: 'true' },
    );

    expect(res.success).toBe(false);
    expect(res.error).toMatch(/함수 실행 중 오류/);
    expect(res.error).toMatch(/의도한 실패/);
  });

  it('정상 코드는 성공으로 보고하고 결과를 돌려준다', async () => {
    const res = await runCustomTask(
      {
        type: 'custom',
        operation: 'execute',
        params: { a: 2, b: 3 },
        functionCode: 'return params.a + params.b;',
      },
      { ALLOW_CUSTOM_WORKLOAD: 'true' },
    );

    expect(res.success).toBe(true);
    expect(res.result).toBe(5);
  });
});
