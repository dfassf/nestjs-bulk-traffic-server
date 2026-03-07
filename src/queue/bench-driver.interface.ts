export interface BenchResult {
  operation: 'write' | 'read';
  count: number;
  totalMs: number;
  avgMs: number;
  opsPerSec: number;
}

export interface BenchDriver {
  init(): Promise<void>;
  destroy(): Promise<void>;
  benchWrite(count: number): Promise<BenchResult>;
  benchRead(count: number): Promise<BenchResult>;
  getRowCount(): Promise<number>;
  reset(): Promise<void>;
}

export const BENCH_DRIVER = Symbol('BENCH_DRIVER');
