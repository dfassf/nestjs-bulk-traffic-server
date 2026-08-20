import { GoEngineBackend } from './go-engine.backend';
import { GoEngineClient } from './go-engine.client';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

function buildTask(overrides: Partial<QueueTask> = {}): QueueTask {
  return {
    id: 7,
    requestId: 'req-7',
    execute: async () => null,
    resolve: () => {},
    reject: () => {},
    timestamp: 1_700_000_000_000,
    priority: 5,
    workloadType: WorkloadType.CPU,
    params: {},
    ...overrides,
  };
}

describe('GoEngineBackend', () => {
  let client: GoEngineClient;
  let backend: GoEngineBackend;

  beforeEach(() => {
    client = {
      execute: jest.fn(),
      healthCheck: jest.fn(),
    } as unknown as GoEngineClient;
    backend = new GoEngineBackend(client);
  });

  it('백엔드 이름은 go 다', () => {
    expect(backend.name).toBe('go');
  });

  it('gRPC 성공 응답을 sync 결과로 옮긴다', async () => {
    (client.execute as jest.Mock).mockResolvedValue({
      taskId: '7',
      success: true,
      result: { value: 123 },
      error: '',
      durationMs: 42,
      engine: 'go',
    });

    const result = await backend.execute(buildTask());

    expect(result.mode).toBe('sync');
    if (result.mode !== 'sync') throw new Error('sync 모드여야 함');
    expect(result.taskId).toBe('7');
    expect(result.success).toBe(true);
    expect(result.result).toEqual({ value: 123 });
    expect(result.durationMs).toBe(42);
    expect(result.backend).toBe('go');
  });

  it('빈 문자열 error 는 undefined 로 정리한다', async () => {
    (client.execute as jest.Mock).mockResolvedValue({
      taskId: '7',
      success: true,
      result: null,
      error: '',
      durationMs: 1,
      engine: 'go',
    });

    const result = await backend.execute(buildTask());
    if (result.mode !== 'sync') throw new Error('sync 모드여야 함');
    expect(result.error).toBeUndefined();
  });

  it('실패 응답의 error 메시지를 그대로 전달한다', async () => {
    (client.execute as jest.Mock).mockResolvedValue({
      taskId: '7',
      success: false,
      result: null,
      error: '워커 타임아웃',
      durationMs: 30000,
      engine: 'go',
    });

    const result = await backend.execute(buildTask());
    if (result.mode !== 'sync') throw new Error('sync 모드여야 함');
    expect(result.success).toBe(false);
    expect(result.error).toBe('워커 타임아웃');
  });

  it('healthCheck 는 엔진의 healthy 값을 따른다', async () => {
    (client.healthCheck as jest.Mock).mockResolvedValue({
      healthy: true,
      version: '1.0.0',
      uptimeSeconds: 10,
    });
    await expect(backend.healthCheck()).resolves.toBe(true);

    (client.healthCheck as jest.Mock).mockResolvedValue({
      healthy: false,
      version: '1.0.0',
      uptimeSeconds: 10,
    });
    await expect(backend.healthCheck()).resolves.toBe(false);
  });

  it('healthCheck 호출이 실패하면 false 를 돌려준다', async () => {
    (client.healthCheck as jest.Mock).mockRejectedValue(new Error('연결 끊김'));
    await expect(backend.healthCheck()).resolves.toBe(false);
  });
});
