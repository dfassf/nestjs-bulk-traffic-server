import { Injectable } from '@nestjs/common';
import { QueueTask } from './interfaces/queue-task.interface';

interface BenchmarkRecord {
  taskId: number;
  workloadType: string;
  nodeDurationMs: number | null;
  goDurationMs: number | null;
  nodeSuccess: boolean;
  goSuccess: boolean;
  winner: 'node' | 'go' | 'tie' | 'none';
  timestamp: number;
}

export interface WorkloadSummary {
  nodeAvg: number;
  goAvg: number;
  nodeCount: number;
  goCount: number;
  winner: 'node' | 'go' | 'tie';
}

@Injectable()
export class BenchmarkService {
  private readonly records: BenchmarkRecord[] = [];
  private readonly maxRecords = 10000;
  private readonly sampleRate: number;

  constructor() {
    this.sampleRate = parseFloat(process.env.BENCHMARK_SAMPLE_RATE || '1.0');
  }

  record(
    task: QueueTask,
    nodeResult: PromiseSettledResult<{ result: unknown; durationMs: number }>,
    goResult: PromiseSettledResult<{ result: unknown; durationMs: number; success?: boolean; error?: string }>,
  ): void {
    if (Math.random() > this.sampleRate) return;

    const nodeSuccess = nodeResult.status === 'fulfilled';
    const goSuccess = goResult.status === 'fulfilled' && (goResult.value as any).success !== false;

    const nodeDuration = nodeSuccess ? nodeResult.value.durationMs : null;
    const goDuration = goResult.status === 'fulfilled' ? goResult.value.durationMs : null;

    let winner: 'node' | 'go' | 'tie' | 'none' = 'none';
    if (nodeSuccess && goSuccess && nodeDuration !== null && goDuration !== null) {
      if (nodeDuration < goDuration) winner = 'node';
      else if (goDuration < nodeDuration) winner = 'go';
      else winner = 'tie';
    } else if (nodeSuccess) {
      winner = 'node';
    } else if (goSuccess) {
      winner = 'go';
    }

    const rec: BenchmarkRecord = {
      taskId: task.id,
      workloadType: task.workloadType || 'unknown',
      nodeDurationMs: nodeDuration,
      goDurationMs: goDuration,
      nodeSuccess,
      goSuccess,
      winner,
      timestamp: Date.now(),
    };

    this.records.push(rec);
    if (this.records.length > this.maxRecords) {
      this.records.splice(0, this.records.length - this.maxRecords);
    }
  }

  getStats() {
    const total = this.records.length;
    if (total === 0) {
      return {
        totalComparisons: 0,
        nodeWins: 0,
        goWins: 0,
        ties: 0,
        summary: { node: null, go: null },
        byWorkloadType: {},
      };
    }

    let nodeWins = 0;
    let goWins = 0;
    let ties = 0;

    let nodeTotal = 0;
    let nodeCount = 0;
    let nodeErrors = 0;
    let goTotal = 0;
    let goCount = 0;
    let goErrors = 0;

    const nodeLatencies: number[] = [];
    const goLatencies: number[] = [];
    const byWorkload = new Map<string, { nodeDurations: number[]; goDurations: number[] }>();

    for (const r of this.records) {
      if (r.winner === 'node') nodeWins++;
      else if (r.winner === 'go') goWins++;
      else if (r.winner === 'tie') ties++;

      if (r.nodeDurationMs !== null) {
        nodeTotal += r.nodeDurationMs;
        nodeCount++;
        nodeLatencies.push(r.nodeDurationMs);
      }
      if (!r.nodeSuccess) nodeErrors++;

      if (r.goDurationMs !== null) {
        goTotal += r.goDurationMs;
        goCount++;
        goLatencies.push(r.goDurationMs);
      }
      if (!r.goSuccess) goErrors++;

      if (!byWorkload.has(r.workloadType)) {
        byWorkload.set(r.workloadType, { nodeDurations: [], goDurations: [] });
      }
      const wl = byWorkload.get(r.workloadType)!;
      if (r.nodeDurationMs !== null) wl.nodeDurations.push(r.nodeDurationMs);
      if (r.goDurationMs !== null) wl.goDurations.push(r.goDurationMs);
    }

    const p99 = (arr: number[]): number => {
      if (arr.length === 0) return 0;
      const sorted = [...arr].sort((a, b) => a - b);
      const idx = Math.ceil(sorted.length * 0.99) - 1;
      return sorted[Math.min(idx, sorted.length - 1)];
    };

    const firstTs = this.records[0]?.timestamp || Date.now();
    const elapsed = (Date.now() - firstTs) / 1000;

    const byWorkloadType: Record<string, WorkloadSummary> = {};
    for (const [type, data] of byWorkload) {
      const nAvg = data.nodeDurations.length > 0
        ? data.nodeDurations.reduce((a, b) => a + b, 0) / data.nodeDurations.length
        : 0;
      const gAvg = data.goDurations.length > 0
        ? data.goDurations.reduce((a, b) => a + b, 0) / data.goDurations.length
        : 0;

      byWorkloadType[type] = {
        nodeAvg: Math.round(nAvg * 10) / 10,
        goAvg: Math.round(gAvg * 10) / 10,
        nodeCount: data.nodeDurations.length,
        goCount: data.goDurations.length,
        winner: nAvg < gAvg ? 'node' : gAvg < nAvg ? 'go' : 'tie',
      };
    }

    return {
      totalComparisons: total,
      nodeWins,
      goWins,
      ties,
      summary: {
        node: {
          avgLatencyMs: nodeCount > 0 ? Math.round((nodeTotal / nodeCount) * 10) / 10 : 0,
          p99LatencyMs: Math.round(p99(nodeLatencies) * 10) / 10,
          errorRate: total > 0 ? Math.round((nodeErrors / total) * 1000) / 1000 : 0,
          throughput: elapsed > 0 ? `${(nodeCount / elapsed).toFixed(1)} tasks/s` : '0 tasks/s',
        },
        go: {
          avgLatencyMs: goCount > 0 ? Math.round((goTotal / goCount) * 10) / 10 : 0,
          p99LatencyMs: Math.round(p99(goLatencies) * 10) / 10,
          errorRate: total > 0 ? Math.round((goErrors / total) * 1000) / 1000 : 0,
          throughput: elapsed > 0 ? `${(goCount / elapsed).toFixed(1)} tasks/s` : '0 tasks/s',
        },
      },
      byWorkloadType,
    };
  }
}
