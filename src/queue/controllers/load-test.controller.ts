import { Body, Controller, Get, Post } from '@nestjs/common';
import { QueueService } from '../queue.service';
import { EngineRouterService } from '../engine-router.service';
import { WorkloadType } from '../interfaces/queue-task.interface';

@Controller('load-test')
export class LoadTestController {
  constructor(
    private readonly queueService: QueueService,
    private readonly engineRouter: EngineRouterService,
  ) {}

  @Post('cpu')
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
