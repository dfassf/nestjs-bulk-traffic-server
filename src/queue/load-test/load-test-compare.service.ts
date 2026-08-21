import { Injectable } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import { EngineRouterService } from '../engine-router.service';
import { WorkloadType } from '../interfaces/queue-task.interface';
import { QueueService } from '../queue.service';
import { SimulationService } from '../simulation.service';
import { summarizeLatencies } from './load-test-metrics.util';

/**
 * 엔진 비교 한 라운드의 결과.
 * 실패 라운드는 지연을 측정하지 못했으므로 null 이다(0 이 아니다).
 */
export interface CompareRound {
  winner: 'node' | 'go' | 'error';
  nodeMs: number | null;
  goMs: number | null;
}

/** 라운드 목록에서 한쪽 엔진의 실측 지연만 추린다(측정 못 한 라운드는 제외). */
function pickLatencies(rounds: CompareRound[], key: 'nodeMs' | 'goMs'): number[] {
  return rounds.map((r) => r[key]).filter((ms): ms is number => ms !== null);
}

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
    const results: CompareRound[] = [];

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
        // 실패 라운드의 지연은 0 이 아니라 측정 불가다. 0 으로 적으면
        // 평균과 차트에서 "가장 빠른 구간"으로 읽힌다.
        results.push({ winner: 'error', nodeMs: null, goMs: null });
      }
    }

    const valid = results.filter((result) => result.winner !== 'error');
    const node = summarizeLatencies(pickLatencies(valid, 'nodeMs'));
    const go = summarizeLatencies(pickLatencies(valid, 'goMs'));

    return {
      total: count,
      task: `findPrimes(max=${max})`,
      nodeWins: results.filter((result) => result.winner === 'node').length,
      goWins: results.filter((result) => result.winner === 'go').length,
      errors: results.filter((result) => result.winner === 'error').length,
      avgNodeMs: node.avgMs,
      avgGoMs: go.avgMs,
      // 표본이 없으면 비교 자체가 성립하지 않으므로 배속도 null.
      speedup:
        node.avgMs !== null && go.avgMs !== null && go.avgMs > 0
          ? Math.round((node.avgMs / go.avgMs) * 100) / 100
          : null,
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

      const results: (CompareRound & { index: number })[] = [];
      const startAll = performance.now();

      for (let i = 0; i < count; i++) {
        // 측정 전 상태는 0 이 아니라 '아직 없음'이다. 실패하면 그대로 null 로 남는다.
        let nodeMs: number | null = null;
        let goMs: number | null = null;
        let winner: CompareRound['winner'] = 'error';

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

          winner =
            nodeMs !== null && goMs !== null
              ? nodeMs <= goMs
                ? 'node'
                : 'go'
              : 'error';
        } catch {
          // 어느 쪽이 얼마나 걸렸는지 모르는 상태다. 측정값을 지워 0 이 남지 않게 한다.
          nodeMs = null;
          goMs = null;
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
          // 누적 평균도 표본이 없으면 null. 아래 done 집계와 같은 규칙을 쓴다.
          avgNodeMs: summarizeLatencies(pickLatencies(valid, 'nodeMs')).avgMs,
          avgGoMs: summarizeLatencies(pickLatencies(valid, 'goMs')).avgMs,
        });
      }

      const totalMs = Math.round((performance.now() - startAll) * 100) / 100;
      const valid = results.filter((result) => result.winner !== 'error');

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
        // 전량 실패하면 각 지표가 null 로 나간다. 0 으로 메우면 두 엔진 다
        // 지연 0ms 로 보여 비교가 성립한 것처럼 읽힌다.
        node: summarizeLatencies(pickLatencies(valid, 'nodeMs')),
        go: summarizeLatencies(pickLatencies(valid, 'goMs')),
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
