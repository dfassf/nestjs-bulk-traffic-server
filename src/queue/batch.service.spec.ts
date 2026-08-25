import { BatchService } from './batch.service';
import { QueueTask, TaskBatch } from './interfaces/queue-task.interface';

describe('BatchService.processBatch', () => {
  let service: BatchService;
  let batchQueues: Map<string, TaskBatch>;

  beforeEach(() => {
    service = new BatchService();
    batchQueues = new Map();
  });

  function makeTask(
    execute: () => Promise<unknown>,
    sink: { resolved: unknown[]; rejected: unknown[] },
  ): QueueTask {
    return {
      id: 1,
      execute,
      resolve: (value: unknown) => sink.resolved.push(value),
      reject: (error: unknown) => sink.rejected.push(error),
      priority: 0,
      category: 'bulk',
      size: 1,
      params: {},
      createdAt: Date.now(),
    } as unknown as QueueTask;
  }

  function seedBatch(tasks: QueueTask[]): void {
    batchQueues.set('bulk', {
      tasks,
      category: 'bulk',
      totalSize: tasks.length,
      createdAt: Date.now(),
    });
  }

  /** onComplete 가 두 번(시작·완료) 불리므로 마지막 호출을 본다. */
  function runBatch(): Promise<{ processed: number; failed: number }> {
    return new Promise((resolve) => {
      const calls: { processed: number; failed: number }[] = [];
      service.processBatch(
        'bulk',
        batchQueues,
        0,
        100,
        (processed, change, failed) => {
          calls.push({ processed, failed: failed ?? 0 });
          // 음수 변화량이 배치가 끝났다는 신호다.
          if (change < 0) resolve(calls[calls.length - 1]);
        },
      );
    });
  }

  it('성공한 작업을 처리 건수로 센다', async () => {
    const sink = { resolved: [] as unknown[], rejected: [] as unknown[] };
    seedBatch([
      makeTask(async () => 'a', sink),
      makeTask(async () => 'b', sink),
    ]);

    const result = await runBatch();

    expect(result.processed).toBe(2);
    expect(result.failed).toBe(0);
    expect(sink.resolved).toEqual(['a', 'b']);
  });

  // 실패를 세지 않으면 배치가 통째로 터져도 통계에는 아무것도 안 남는다.
  // 화면에서는 작업이 사라진 것처럼 보인다.
  it('실패한 작업을 실패 건수로 센다', async () => {
    const sink = { resolved: [] as unknown[], rejected: [] as unknown[] };
    seedBatch([
      makeTask(async () => {
        throw new Error('실패');
      }, sink),
      makeTask(async () => {
        throw new Error('실패');
      }, sink),
    ]);

    const result = await runBatch();

    expect(result.processed).toBe(0);
    expect(result.failed).toBe(2);
    expect(sink.rejected).toHaveLength(2);
  });

  it('성공과 실패가 섞이면 합이 전체 건수와 맞는다', async () => {
    const sink = { resolved: [] as unknown[], rejected: [] as unknown[] };
    seedBatch([
      makeTask(async () => 'ok', sink),
      makeTask(async () => {
        throw new Error('실패');
      }, sink),
      makeTask(async () => 'ok', sink),
    ]);

    const result = await runBatch();

    expect(result.processed).toBe(2);
    expect(result.failed).toBe(1);
    // 어느 쪽으로든 반드시 세어져야 한다. 빠지면 합이 안 맞는다.
    expect(result.processed + result.failed).toBe(3);
  });

  it('빈 배치는 큐에서 지우고 아무것도 하지 않는다', () => {
    const onComplete = jest.fn();
    seedBatch([]);

    service.processBatch('bulk', batchQueues, 0, 100, onComplete);

    expect(batchQueues.has('bulk')).toBe(false);
    expect(onComplete).not.toHaveBeenCalled();
  });

  it('여유 슬롯이 없으면 처리하지 않는다', () => {
    const sink = { resolved: [] as unknown[], rejected: [] as unknown[] };
    const onComplete = jest.fn();
    seedBatch([makeTask(async () => 'a', sink)]);

    service.processBatch('bulk', batchQueues, 100, 100, onComplete);

    expect(onComplete).not.toHaveBeenCalled();
    expect(batchQueues.get('bulk')?.tasks).toHaveLength(1);
  });
});
