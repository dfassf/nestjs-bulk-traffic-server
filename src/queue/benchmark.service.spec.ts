import { BenchmarkService } from './benchmark.service';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

function makeTask(overrides: Partial<QueueTask> = {}): QueueTask {
  return {
    id: 1,
    execute: async () => null,
    resolve: () => {},
    reject: () => {},
    timestamp: Date.now(),
    priority: 5,
    workloadType: WorkloadType.CPU,
    ...overrides,
  };
}

describe('BenchmarkService', () => {
  let service: BenchmarkService;

  beforeEach(() => {
    process.env.BENCHMARK_SAMPLE_RATE = '1.0';
    service = new BenchmarkService();
  });

  it('should return empty stats when no records', () => {
    const stats = service.getStats();
    expect(stats.totalComparisons).toBe(0);
    expect(stats.nodeWins).toBe(0);
    expect(stats.goWins).toBe(0);
  });

  it('should record node win when node is faster', () => {
    const task = makeTask({ id: 1 });

    service.record(
      task,
      { status: 'fulfilled', value: { result: 'ok', durationMs: 10 } },
      { status: 'fulfilled', value: { result: 'ok', durationMs: 50, success: true } },
    );

    const stats = service.getStats();
    expect(stats.totalComparisons).toBe(1);
    expect(stats.nodeWins).toBe(1);
    expect(stats.goWins).toBe(0);
  });

  it('should record go win when go is faster', () => {
    const task = makeTask({ id: 2 });

    service.record(
      task,
      { status: 'fulfilled', value: { result: 'ok', durationMs: 100 } },
      { status: 'fulfilled', value: { result: 'ok', durationMs: 20, success: true } },
    );

    const stats = service.getStats();
    expect(stats.goWins).toBe(1);
  });

  it('should record node win when go fails', () => {
    const task = makeTask({ id: 3 });

    service.record(
      task,
      { status: 'fulfilled', value: { result: 'ok', durationMs: 50 } },
      { status: 'rejected', reason: new Error('go failed') },
    );

    const stats = service.getStats();
    expect(stats.nodeWins).toBe(1);
  });

  it('should track by workload type', () => {
    service.record(
      makeTask({ id: 1, workloadType: WorkloadType.CPU }),
      { status: 'fulfilled', value: { result: 'ok', durationMs: 100 } },
      { status: 'fulfilled', value: { result: 'ok', durationMs: 20, success: true } },
    );

    service.record(
      makeTask({ id: 2, workloadType: WorkloadType.MEMORY }),
      { status: 'fulfilled', value: { result: 'ok', durationMs: 50 } },
      { status: 'fulfilled', value: { result: 'ok', durationMs: 80, success: true } },
    );

    const stats = service.getStats();
    expect(stats.byWorkloadType.cpu).toBeDefined();
    expect(stats.byWorkloadType.cpu.winner).toBe('go');
    expect(stats.byWorkloadType.memory).toBeDefined();
    expect(stats.byWorkloadType.memory.winner).toBe('node');
  });

  it('should calculate summary stats correctly', () => {
    for (let i = 0; i < 10; i++) {
      service.record(
        makeTask({ id: i }),
        { status: 'fulfilled', value: { result: 'ok', durationMs: 100 } },
        { status: 'fulfilled', value: { result: 'ok', durationMs: 30, success: true } },
      );
    }

    const stats = service.getStats();
    expect(stats.summary.node!.avgLatencyMs).toBe(100);
    expect(stats.summary.go!.avgLatencyMs).toBe(30);
    expect(stats.totalComparisons).toBe(10);
    expect(stats.goWins).toBe(10);
  });
});
