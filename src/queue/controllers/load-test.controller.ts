import { Body, Controller, Delete, Get, HttpCode, Post, Sse } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import { QueueService } from '../queue.service';
import { EngineRouterService } from '../engine-router.service';
import { WorkloadType } from '../interfaces/queue-task.interface';
import { SqliteBenchService } from '../sqlite-bench.service';

interface SseMessage {
  data: string;
}

@Controller('load-test')
export class LoadTestController {
  constructor(
    private readonly queueService: QueueService,
    private readonly engineRouter: EngineRouterService,
    private readonly sqliteBench: SqliteBenchService,
  ) {}

  @Post('cpu')
  @HttpCode(200)
  async cpuTask(
    @Body() body: { iterations?: number; priority?: number },
  ) {
    const iterations = body.iterations || 1000;
    const priority = body.priority ?? 0;

    return this.queueService.enqueue(
      () => this.simulateCPU(iterations),
      { priority, workloadType: WorkloadType.CPU, params: { iterations } },
    );
  }

  @Post('io')
  @HttpCode(200)
  async ioTask(
    @Body() body: { delayMs?: number; priority?: number },
  ) {
    const delayMs = body.delayMs || 100;
    const priority = body.priority ?? 0;

    return this.queueService.enqueue(
      () => this.simulateIO(delayMs),
      { priority, params: { delay_ms: delayMs } },
    );
  }

  @Post('batch')
  @HttpCode(200)
  async batchTask(
    @Body() body: { itemCount?: number; priority?: number },
  ) {
    const itemCount = body.itemCount || 10;
    const priority = body.priority ?? -3;

    return this.queueService.enqueue(
      () => this.simulateBatch(itemCount),
      { priority, params: { item_count: itemCount }, batch: true, category: 'load-test-batch' },
    );
  }

  @Post('mixed')
  @HttpCode(200)
  async mixedBurst(
    @Body() body: { count?: number; cpuRatio?: number; ioRatio?: number; batchRatio?: number },
  ) {
    const count = Math.min(body.count || 10, 200);
    const cpuRatio = body.cpuRatio ?? 0.4;
    const ioRatio = body.ioRatio ?? 0.4;

    const tasks: Promise<unknown>[] = [];
    for (let i = 0; i < count; i++) {
      const rand = Math.random();
      if (rand < cpuRatio) {
        tasks.push(this.queueService.enqueue(() => this.simulateCPU(500), {
          priority: Math.floor(Math.random() * 10) - 3,
          workloadType: WorkloadType.CPU,
          params: { iterations: 500 },
        }));
      } else if (rand < cpuRatio + ioRatio) {
        tasks.push(this.queueService.enqueue(() => this.simulateIO(50), {
          priority: Math.floor(Math.random() * 10) - 3,
          params: { delay_ms: 50 },
        }));
      } else {
        tasks.push(this.queueService.enqueue(() => this.simulateBatch(5), {
          priority: -5,
          params: { item_count: 5 },
          batch: true,
          category: 'load-test-batch',
        }));
      }
    }

    const results = await Promise.allSettled(tasks);
    const fulfilled = results.filter(r => r.status === 'fulfilled').length;
    const rejected = results.filter(r => r.status === 'rejected').length;

    return { total: count, fulfilled, rejected };
  }

  @Post('compare')
  @HttpCode(200)
  async compareEngines(
    @Body() body: { count?: number; max?: number },
  ) {
    if (this.engineRouter.getEngine() !== 'both') {
      return { error: 'WORKER_ENGINE=both 모드에서만 사용 가능합니다.' };
    }

    const count = Math.min(body.count || 10, 100);
    const max = body.max || 500000;
    const results: { winner: string; nodeMs: number; goMs: number }[] = [];

    for (let i = 0; i < count; i++) {
      const goTask = {
        id: Date.now() + i,
        requestId: `compare-go-${i}`,
        timestamp: Date.now(),
        priority: 0,
        workloadType: 'cpu' as any,
        params: { max },
        timeout: 30000,
      } as any;

      try {
        // Node: worker thread 경유 (findPrimes via QueueService)
        const nodeStart = performance.now();
        await this.queueService.enqueue(
          () => Promise.resolve(),
          { priority: 5, workloadType: WorkloadType.CPU, params: { max } },
        );
        const nodeMs = performance.now() - nodeStart;

        // Go: gRPC → goroutine 경유
        const goStart = performance.now();
        await this.engineRouter.dispatchToGo(goTask);
        const goMs = performance.now() - goStart;

        results.push({
          winner: nodeMs <= goMs ? 'node' : 'go',
          nodeMs: Math.round(nodeMs * 100) / 100,
          goMs: Math.round(goMs * 100) / 100,
        });
      } catch (e) {
        results.push({ winner: 'error', nodeMs: 0, goMs: 0 });
      }
    }

    const nodeWins = results.filter(r => r.winner === 'node').length;
    const goWins = results.filter(r => r.winner === 'go').length;
    const errors = results.filter(r => r.winner === 'error').length;
    const validResults = results.filter(r => r.winner !== 'error');
    const avgNodeMs = validResults.length > 0
      ? validResults.reduce((s, r) => s + r.nodeMs, 0) / validResults.length : 0;
    const avgGoMs = validResults.length > 0
      ? validResults.reduce((s, r) => s + r.goMs, 0) / validResults.length : 0;

    return {
      total: count,
      task: `findPrimes(max=${max})`,
      nodeWins,
      goWins,
      errors,
      avgNodeMs: Math.round(avgNodeMs * 100) / 100,
      avgGoMs: Math.round(avgGoMs * 100) / 100,
      speedup: avgGoMs > 0 ? Math.round((avgNodeMs / avgGoMs) * 100) / 100 : null,
      detail: results,
    };
  }

  @Post('db-write')
  @HttpCode(200)
  dbWrite(@Body() body: { count?: number }) {
    const count = Math.min(body.count || 100, 100_000_000);
    return this.sqliteBench.benchWrite(count);
  }

  @Post('db-read')
  @HttpCode(200)
  dbRead(@Body() body: { count?: number }) {
    const count = Math.min(body.count || 100, 100_000_000);
    return this.sqliteBench.benchRead(count);
  }

  @Get('db-rows')
  dbRows() {
    return { rows: this.sqliteBench.getRowCount() };
  }

  @Delete('db-reset')
  dbReset() {
    this.sqliteBench.reset();
    return { ok: true };
  }

  @Get('ping')
  ping() {
    return { ok: true, engine: this.engineRouter.getEngine(), timestamp: Date.now() };
  }

  @Post('run-stream')
  @HttpCode(200)
  @Sse()
  runStream(
    @Body() body: {
      type: 'cpu' | 'io' | 'mixed' | 'db-write' | 'db-read';
      count?: number;
      iterations?: number;
      delayMs?: number;
      cpuRatio?: number;
      ioRatio?: number;
      max?: number;
    },
  ): Observable<MessageEvent> {
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
          fulfilled: results.filter(r => r.ok).length,
          rejected: results.filter(r => !r.ok).length,
        });
      }

      const totalMs = Math.round((performance.now() - startAll) * 100) / 100;
      const fulfilled = results.filter(r => r.ok);
      const rejected = results.filter(r => !r.ok);
      const durations = fulfilled.map(r => r.ms).sort((a, b) => a - b);

      emit('done', {
        type: testType,
        total: count,
        fulfilled: fulfilled.length,
        rejected: rejected.length,
        totalMs,
        avgMs: durations.length > 0
          ? Math.round((durations.reduce((s, v) => s + v, 0) / durations.length) * 100) / 100
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

  @Post('compare-stream')
  @HttpCode(200)
  @Sse()
  compareStream(
    @Body() body: { count?: number; max?: number },
  ): Observable<MessageEvent> {
    const subject = new Subject<MessageEvent>();
    const count = Math.min(body.count || 20, 200);
    const max = body.max || 500_000;

    const emit = (event: string, payload: Record<string, unknown>) => {
      subject.next({ data: JSON.stringify({ event, ...payload }) } as MessageEvent);
    };

    const run = async () => {
      const engineMode = this.engineRouter.getEngine();
      emit('start', { count, max, engine: engineMode, timestamp: Date.now() });

      if (engineMode !== 'both') {
        emit('error', { message: 'WORKER_ENGINE=both 모드에서만 사용 가능합니다.' });
        subject.complete();
        return;
      }

      const results: { index: number; nodeMs: number; goMs: number; winner: string }[] = [];
      const startAll = performance.now();

      for (let i = 0; i < count; i++) {
        let nodeMs = 0;
        let goMs = 0;
        let winner = 'error';

        try {
          const nodeStart = performance.now();
          await this.queueService.enqueue(
            () => Promise.resolve(),
            { priority: 5, workloadType: WorkloadType.CPU, params: { max } },
          );
          nodeMs = Math.round((performance.now() - nodeStart) * 100) / 100;

          const goTask = {
            id: Date.now() + i,
            requestId: `compare-go-${i}`,
            timestamp: Date.now(),
            priority: 0,
            workloadType: 'cpu' as any,
            params: { max },
            timeout: 30000,
          } as any;

          const goStart = performance.now();
          await this.engineRouter.dispatchToGo(goTask);
          goMs = Math.round((performance.now() - goStart) * 100) / 100;

          winner = nodeMs <= goMs ? 'node' : 'go';
        } catch {
          winner = 'error';
        }

        results.push({ index: i, nodeMs, goMs, winner });

        const valid = results.filter(r => r.winner !== 'error');
        emit('progress', {
          index: i,
          total: count,
          nodeMs,
          goMs,
          winner,
          nodeWins: results.filter(r => r.winner === 'node').length,
          goWins: results.filter(r => r.winner === 'go').length,
          errors: results.filter(r => r.winner === 'error').length,
          avgNodeMs: valid.length > 0
            ? Math.round((valid.reduce((s, r) => s + r.nodeMs, 0) / valid.length) * 100) / 100 : 0,
          avgGoMs: valid.length > 0
            ? Math.round((valid.reduce((s, r) => s + r.goMs, 0) / valid.length) * 100) / 100 : 0,
        });
      }

      const totalMs = Math.round((performance.now() - startAll) * 100) / 100;
      const valid = results.filter(r => r.winner !== 'error');
      const nodeDurations = valid.map(r => r.nodeMs).sort((a, b) => a - b);
      const goDurations = valid.map(r => r.goMs).sort((a, b) => a - b);

      const pctl = (arr: number[], p: number) => arr[Math.floor(arr.length * p)] ?? 0;

      emit('done', {
        total: count,
        task: `findPrimes(max=${max})`,
        totalMs,
        nodeWins: results.filter(r => r.winner === 'node').length,
        goWins: results.filter(r => r.winner === 'go').length,
        errors: results.filter(r => r.winner === 'error').length,
        node: {
          avgMs: valid.length > 0
            ? Math.round((valid.reduce((s, r) => s + r.nodeMs, 0) / valid.length) * 100) / 100 : 0,
          p50: pctl(nodeDurations, 0.5),
          p95: pctl(nodeDurations, 0.95),
          p99: pctl(nodeDurations, 0.99),
          min: nodeDurations[0] ?? 0,
          max: nodeDurations[nodeDurations.length - 1] ?? 0,
        },
        go: {
          avgMs: valid.length > 0
            ? Math.round((valid.reduce((s, r) => s + r.goMs, 0) / valid.length) * 100) / 100 : 0,
          p50: pctl(goDurations, 0.5),
          p95: pctl(goDurations, 0.95),
          p99: pctl(goDurations, 0.99),
          min: goDurations[0] ?? 0,
          max: goDurations[goDurations.length - 1] ?? 0,
        },
      });

      subject.complete();
    };

    void run();
    return subject.asObservable();
  }

  private async dispatchSingleTask(
    type: string,
    body: { iterations?: number; delayMs?: number; cpuRatio?: number; ioRatio?: number; max?: number; count?: number },
  ): Promise<unknown> {
    switch (type) {
      case 'cpu':
        return this.queueService.enqueue(
          () => this.simulateCPU(body.iterations || 1000),
          { priority: 0, workloadType: WorkloadType.CPU, params: { iterations: body.iterations || 1000 } },
        );
      case 'io':
        return this.queueService.enqueue(
          () => this.simulateIO(body.delayMs || 100),
          { priority: 0, params: { delay_ms: body.delayMs || 100 } },
        );
      case 'db-write':
        return this.sqliteBench.benchWrite(body.count || 100);
      case 'db-read':
        return this.sqliteBench.benchRead(body.count || 100);
      case 'mixed':
      default: {
        const cpuRatio = body.cpuRatio ?? 0.5;
        const ioRatio = body.ioRatio ?? 0.5;
        const rand = Math.random();
        if (rand < cpuRatio) {
          return this.queueService.enqueue(() => this.simulateCPU(500), {
            priority: 0, workloadType: WorkloadType.CPU, params: { iterations: 500 },
          });
        } else if (rand < cpuRatio + ioRatio) {
          return this.queueService.enqueue(() => this.simulateIO(50), {
            priority: 0, params: { delay_ms: 50 },
          });
        } else {
          return this.queueService.enqueue(() => this.simulateBatch(5), {
            priority: -5, params: { item_count: 5 }, batch: true, category: 'load-test-batch',
          });
        }
      }
    }
  }

  private simulateCPU(iterations: number): Promise<{ hash: string; iterations: number }> {
    return new Promise((resolve) => {
      let hash = 0;
      for (let i = 0; i < iterations; i++) {
        hash = ((hash << 5) - hash + i) | 0;
      }
      resolve({ hash: hash.toString(16), iterations });
    });
  }

  private simulateIO(delayMs: number): Promise<{ status: string; delayMs: number }> {
    return new Promise((resolve) => {
      setTimeout(() => resolve({ status: 'completed', delayMs }), delayMs);
    });
  }

  private simulateBatch(itemCount: number): Promise<{ processed: number; items: string[] }> {
    return new Promise((resolve) => {
      const items: string[] = [];
      let done = 0;
      const processOne = () => {
        items.push(`item-${done}`);
        done++;
        if (done >= itemCount) {
          resolve({ processed: done, items: items.slice(0, 3) });
        } else {
          setTimeout(processOne, 5);
        }
      };
      processOne();
    });
  }
}
