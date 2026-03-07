import { Injectable } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import { EngineRouterService } from '../engine-router.service';
import { WorkloadType } from '../interfaces/queue-task.interface';
import { QueueService } from '../queue.service';
import { SimulationService } from '../simulation.service';

@Injectable()
export class LoadTestCompareService {
  constructor(
    private readonly queueService: QueueService,
    private readonly engineRouter: EngineRouterService,
    private readonly simulation: SimulationService,
  ) {}

  async compareEngines(body: { count?: number; max?: number }) {
    if (this.engineRouter.getEngine() !== 'both') {
      return { error: 'WORKER_ENGINE=both 모드에서만 사용 가능합니다.' };
    }

    const count = Math.min(body.count || 10, 100);
    const max = body.max || 500000;
    const results: { winner: string; nodeMs: number; goMs: number }[] = [];

    for (let i = 0; i < count; i++) {
      try {
        const nodeStart = performance.now();
        await this.queueService.enqueue(() => Promise.resolve(), {
          priority: 5,
          workloadType: WorkloadType.CPU,
          params: { max },
        });
        const nodeMs = Math.round((performance.now() - nodeStart) * 100) / 100;

        const goStart = performance.now();
        await this.engineRouter.dispatchToGo(
          this.createGoTask(i, 'cpu', { max }, 30000, 'compare-go'),
        );
        const goMs = Math.round((performance.now() - goStart) * 100) / 100;

        results.push({
          winner: nodeMs <= goMs ? 'node' : 'go',
          nodeMs,
          goMs,
        });
      } catch {
        results.push({ winner: 'error', nodeMs: 0, goMs: 0 });
      }
    }

    const validResults = results.filter((result) => result.winner !== 'error');
    const avgNodeMs =
      validResults.length > 0
        ? validResults.reduce((sum, result) => sum + result.nodeMs, 0) /
          validResults.length
        : 0;
    const avgGoMs =
      validResults.length > 0
        ? validResults.reduce((sum, result) => sum + result.goMs, 0) /
          validResults.length
        : 0;

    return {
      total: count,
      task: `findPrimes(max=${max})`,
      nodeWins: results.filter((result) => result.winner === 'node').length,
      goWins: results.filter((result) => result.winner === 'go').length,
      errors: results.filter((result) => result.winner === 'error').length,
      avgNodeMs: Math.round(avgNodeMs * 100) / 100,
      avgGoMs: Math.round(avgGoMs * 100) / 100,
      speedup: avgGoMs > 0 ? Math.round((avgNodeMs / avgGoMs) * 100) / 100 : null,
      detail: results,
    };
  }

  compareStream(body: {
    count?: number;
    max?: number;
    testType?: 'cpu' | 'io';
    delayMs?: number;
  }): Observable<MessageEvent> {
    const subject = new Subject<MessageEvent>();
    const count = Math.min(body.count || 20, 200);
    const max = body.max || 500_000;
    const testType = body.testType || 'cpu';
    const delayMs = body.delayMs || 100;

    const emit = (event: string, payload: Record<string, unknown>) => {
      subject.next({ data: JSON.stringify({ event, ...payload }) } as MessageEvent);
    };

    const run = async () => {
      const engineMode = this.engineRouter.getEngine();
      emit('start', {
        count,
        max,
        testType,
        delayMs,
        engine: engineMode,
        timestamp: Date.now(),
      });

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
          if (testType === 'io') {
            const nodeStart = performance.now();
            await this.simulation.simulateIO(delayMs);
            nodeMs = Math.round((performance.now() - nodeStart) * 100) / 100;

            const goStart = performance.now();
            await this.engineRouter.dispatchToGo(
              this.createGoTask(i, 'io', { delay_ms: delayMs }, 60000, 'compare-io-go'),
            );
            goMs = Math.round((performance.now() - goStart) * 100) / 100;
          } else {
            const nodeStart = performance.now();
            await this.queueService.enqueue(() => Promise.resolve(), {
              priority: 5,
              workloadType: WorkloadType.CPU,
              params: { max },
            });
            nodeMs = Math.round((performance.now() - nodeStart) * 100) / 100;

            const goStart = performance.now();
            await this.engineRouter.dispatchToGo(
              this.createGoTask(i, 'cpu', { max }, 30000, 'compare-go'),
            );
            goMs = Math.round((performance.now() - goStart) * 100) / 100;
          }

          winner = nodeMs <= goMs ? 'node' : 'go';
        } catch {
          winner = 'error';
        }

        results.push({ index: i, nodeMs, goMs, winner });

        const valid = results.filter((result) => result.winner !== 'error');
        emit('progress', {
          index: i,
          total: count,
          nodeMs,
          goMs,
          winner,
          nodeWins: results.filter((result) => result.winner === 'node').length,
          goWins: results.filter((result) => result.winner === 'go').length,
          errors: results.filter((result) => result.winner === 'error').length,
          avgNodeMs:
            valid.length > 0
              ? Math.round(
                  (valid.reduce((sum, result) => sum + result.nodeMs, 0) /
                    valid.length) *
                    100,
                ) / 100
              : 0,
          avgGoMs:
            valid.length > 0
              ? Math.round(
                  (valid.reduce((sum, result) => sum + result.goMs, 0) /
                    valid.length) *
                    100,
                ) / 100
              : 0,
        });
      }

      const totalMs = Math.round((performance.now() - startAll) * 100) / 100;
      const valid = results.filter((result) => result.winner !== 'error');
      const nodeDurations = valid.map((result) => result.nodeMs).sort((a, b) => a - b);
      const goDurations = valid.map((result) => result.goMs).sort((a, b) => a - b);
      const pctl = (arr: number[], p: number) => arr[Math.floor(arr.length * p)] ?? 0;

      const taskLabel =
        testType === 'io' ? `asyncIO(delay=${delayMs}ms)` : `findPrimes(max=${max})`;

      emit('done', {
        total: count,
        testType,
        task: taskLabel,
        totalMs,
        nodeWins: results.filter((result) => result.winner === 'node').length,
        goWins: results.filter((result) => result.winner === 'go').length,
        errors: results.filter((result) => result.winner === 'error').length,
        node: {
          avgMs:
            valid.length > 0
              ? Math.round(
                  (valid.reduce((sum, result) => sum + result.nodeMs, 0) /
                    valid.length) *
                    100,
                ) / 100
              : 0,
          p50: pctl(nodeDurations, 0.5),
          p95: pctl(nodeDurations, 0.95),
          p99: pctl(nodeDurations, 0.99),
          min: nodeDurations[0] ?? 0,
          max: nodeDurations[nodeDurations.length - 1] ?? 0,
        },
        go: {
          avgMs:
            valid.length > 0
              ? Math.round(
                  (valid.reduce((sum, result) => sum + result.goMs, 0) /
                    valid.length) *
                    100,
                ) / 100
              : 0,
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

  private createGoTask(
    index: number,
    workloadType: 'cpu' | 'io',
    params: Record<string, unknown>,
    timeout: number,
    requestIdPrefix: string,
  ) {
    return {
      id: Date.now() + index,
      requestId: `${requestIdPrefix}-${index}`,
      timestamp: Date.now(),
      priority: 0,
      workloadType: workloadType as any,
      params,
      timeout,
    } as any;
  }
}
