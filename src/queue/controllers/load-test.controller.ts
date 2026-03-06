import { Body, Controller, Delete, Get, HttpCode, Post } from '@nestjs/common';
import { QueueService } from '../queue.service';
import { EngineRouterService } from '../engine-router.service';
import { WorkloadType } from '../interfaces/queue-task.interface';
import { SqliteBenchService } from '../sqlite-bench.service';

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
