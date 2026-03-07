import { Inject, Injectable } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import { BENCH_DRIVER, BenchDriver } from '../bench-driver.interface';
import { WorkloadType } from '../interfaces/queue-task.interface';
import { QueueService } from '../queue.service';
import { SimulationService } from '../simulation.service';

type StreamTaskType = 'cpu' | 'io' | 'mixed' | 'db-write' | 'db-read';

@Injectable()
export class LoadTestRunnerService {
  constructor(
    private readonly queueService: QueueService,
    private readonly simulation: SimulationService,
    @Inject(BENCH_DRIVER) private readonly bench: BenchDriver,
  ) {}

  async enqueueCpuTask(body: { iterations?: number; priority?: number }) {
    const iterations = body.iterations || 1000;
    const priority = body.priority ?? 0;

    return this.queueService.enqueue(
      () => this.simulation.simulateCPU(iterations),
      { priority, workloadType: WorkloadType.CPU, params: { iterations } },
    );
  }

  async enqueueIoTask(body: { delayMs?: number; priority?: number }) {
    const delayMs = body.delayMs || 100;
    const priority = body.priority ?? 0;

    return this.queueService.enqueue(
      () => this.simulation.simulateIO(delayMs),
      { priority, params: { delay_ms: delayMs } },
    );
  }

  async enqueueBatchTask(body: { itemCount?: number; priority?: number }) {
    const itemCount = body.itemCount || 10;
    const priority = body.priority ?? -3;

    return this.queueService.enqueue(
      () => this.simulation.simulateBatch(itemCount),
      {
        priority,
        params: { item_count: itemCount },
        batch: true,
        category: 'load-test-batch',
      },
    );
  }

  async runMixedBurst(body: {
    count?: number;
    cpuRatio?: number;
    ioRatio?: number;
  }) {
    const count = Math.min(body.count || 10, 200);
    const cpuRatio = body.cpuRatio ?? 0.4;
    const ioRatio = body.ioRatio ?? 0.4;

    const tasks: Promise<unknown>[] = [];
    for (let i = 0; i < count; i++) {
      const rand = Math.random();

      if (rand < cpuRatio) {
        tasks.push(
          this.queueService.enqueue(() => this.simulation.simulateCPU(500), {
            priority: Math.floor(Math.random() * 10) - 3,
            workloadType: WorkloadType.CPU,
            params: { iterations: 500 },
          }),
        );
      } else if (rand < cpuRatio + ioRatio) {
        tasks.push(
          this.queueService.enqueue(() => this.simulation.simulateIO(50), {
            priority: Math.floor(Math.random() * 10) - 3,
            params: { delay_ms: 50 },
          }),
        );
      } else {
        tasks.push(
          this.queueService.enqueue(() => this.simulation.simulateBatch(5), {
            priority: -5,
            params: { item_count: 5 },
            batch: true,
            category: 'load-test-batch',
          }),
        );
      }
    }

    const results = await Promise.allSettled(tasks);
    return {
      total: count,
      fulfilled: results.filter((result) => result.status === 'fulfilled').length,
      rejected: results.filter((result) => result.status === 'rejected').length,
    };
  }

  dbWrite(body: { count?: number }) {
    const count = Math.min(body.count || 100, 100_000_000);
    return this.bench.benchWrite(count);
  }

  dbRead(body: { count?: number }) {
    const count = Math.min(body.count || 100, 100_000_000);
    return this.bench.benchRead(count);
  }

  async dbRows() {
    return { rows: await this.bench.getRowCount() };
  }

  async dbReset() {
    await this.bench.reset();
    return { ok: true };
  }

  runStream(body: {
    type: StreamTaskType;
    count?: number;
    iterations?: number;
    delayMs?: number;
    cpuRatio?: number;
    ioRatio?: number;
    max?: number;
  }): Observable<MessageEvent> {
    const subject = new Subject<MessageEvent>();
    const count = Math.min(body.count || 20, 500);
    const testType = body.type || 'cpu';

    const emit = (event: string, payload: Record<string, unknown>) => {
      subject.next({ data: JSON.stringify({ event, ...payload }) } as MessageEvent);
    };

    const run = async () => {
      emit('start', { type: testType, count, timestamp: Date.now() });

      const results: { index: number; ok: boolean; ms: number }[] = [];
      const startAll = performance.now();

      for (let i = 0; i < count; i++) {
        const t0 = performance.now();
        let ok = true;

        try {
          await this.dispatchSingleTask(testType, body);
        } catch {
          ok = false;
        }

        const ms = Math.round((performance.now() - t0) * 100) / 100;
        results.push({ index: i, ok, ms });

        emit('progress', {
          index: i,
          total: count,
          ok,
          ms,
          fulfilled: results.filter((result) => result.ok).length,
          rejected: results.filter((result) => !result.ok).length,
        });
      }

      const totalMs = Math.round((performance.now() - startAll) * 100) / 100;
      const fulfilled = results.filter((result) => result.ok);
      const rejected = results.filter((result) => !result.ok);
      const durations = fulfilled.map((result) => result.ms).sort((a, b) => a - b);

      emit('done', {
        type: testType,
        total: count,
        fulfilled: fulfilled.length,
        rejected: rejected.length,
        totalMs,
        avgMs:
          durations.length > 0
            ? Math.round(
                (durations.reduce((sum, value) => sum + value, 0) / durations.length) *
                  100,
              ) / 100
            : 0,
        p50: durations[Math.floor(durations.length * 0.5)] ?? 0,
        p95: durations[Math.floor(durations.length * 0.95)] ?? 0,
        p99: durations[Math.floor(durations.length * 0.99)] ?? 0,
        minMs: durations[0] ?? 0,
        maxMs: durations[durations.length - 1] ?? 0,
      });

      subject.complete();
    };

    void run();
    return subject.asObservable();
  }

  private async dispatchSingleTask(
    type: StreamTaskType,
    body: {
      iterations?: number;
      delayMs?: number;
      cpuRatio?: number;
      ioRatio?: number;
      max?: number;
      count?: number;
    },
  ): Promise<unknown> {
    switch (type) {
      case 'cpu':
        return this.queueService.enqueue(
          () => this.simulation.simulateCPU(body.iterations || 1000),
          {
            priority: 0,
            workloadType: WorkloadType.CPU,
            params: { iterations: body.iterations || 1000 },
          },
        );
      case 'io':
        return this.queueService.enqueue(
          () => this.simulation.simulateIO(body.delayMs || 100),
          { priority: 0, params: { delay_ms: body.delayMs || 100 } },
        );
      case 'db-write':
        return this.bench.benchWrite(body.count || 100);
      case 'db-read':
        return this.bench.benchRead(body.count || 100);
      case 'mixed':
      default: {
        const cpuRatio = body.cpuRatio ?? 0.5;
        const ioRatio = body.ioRatio ?? 0.5;
        const rand = Math.random();

        if (rand < cpuRatio) {
          return this.queueService.enqueue(() => this.simulation.simulateCPU(500), {
            priority: 0,
            workloadType: WorkloadType.CPU,
            params: { iterations: 500 },
          });
        }

        if (rand < cpuRatio + ioRatio) {
          return this.queueService.enqueue(() => this.simulation.simulateIO(50), {
            priority: 0,
            params: { delay_ms: 50 },
          });
        }

        return this.queueService.enqueue(() => this.simulation.simulateBatch(5), {
          priority: -5,
          params: { item_count: 5 },
          batch: true,
          category: 'load-test-batch',
        });
      }
    }
  }
}
