import { BenchDriver, BenchResult } from './bench-driver.interface';
import { Pool } from 'pg';
import {
  readPositiveIntEnv,
  requireEnv,
  requirePositiveIntEnv,
} from './utils/env';

export class PgBenchDriver implements BenchDriver {
  private pool: Pool;

  constructor() {
    // 접속 대상은 기본값을 두지 않는다. 값이 빠졌는데 localhost 로 붙어버리면
    // 원격 DB 를 측정한다고 믿으면서 실제로는 로컬을 재는 상황이 조용히 생긴다.
    // 풀 크기는 성능 조절값이라 틀려도 대상이 바뀌지 않으므로 기본값을 둔다.
    this.pool = new Pool({
      host: requireEnv('BENCH_PG_HOST'),
      port: requirePositiveIntEnv('BENCH_PG_PORT'),
      database: requireEnv('BENCH_PG_DATABASE'),
      user: requireEnv('BENCH_PG_USER'),
      password: requireEnv('BENCH_PG_PASSWORD'),
      max: readPositiveIntEnv('BENCH_PG_POOL_SIZE', 10),
    });
  }

  async init(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS bench_records (
        id SERIAL PRIMARY KEY,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        created_at BIGINT NOT NULL
      )
    `);
    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bench_key ON bench_records(key)
    `);
  }

  async destroy(): Promise<void> {
    await this.pool.end();
  }

  async benchWrite(count: number): Promise<BenchResult> {
    const start = performance.now();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const stmt =
        'INSERT INTO bench_records (key, value, created_at) VALUES ($1, $2, $3)';
      for (let i = 0; i < count; i++) {
        await client.query(stmt, [
          `key-${Date.now()}-${i}`,
          JSON.stringify({ i, data: 'x'.repeat(100) }),
          Date.now(),
        ]);
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
    const totalMs = performance.now() - start;

    return {
      operation: 'write',
      count,
      totalMs: Math.round(totalMs * 100) / 100,
      avgMs: Math.round((totalMs / count) * 1000) / 1000,
      opsPerSec: Math.round((count / totalMs) * 1000),
    };
  }

  async benchRead(count: number): Promise<BenchResult> {
    const res = await this.pool.query(
      'SELECT COUNT(*) as cnt FROM bench_records',
    );
    const totalRows = parseInt(res.rows[0].cnt);
    if (totalRows === 0) {
      return {
        operation: 'read',
        count: 0,
        totalMs: 0,
        avgMs: 0,
        opsPerSec: 0,
      };
    }

    const start = performance.now();
    for (let i = 0; i < count; i++) {
      if (i % 2 === 0) {
        await this.pool.query(
          'SELECT * FROM bench_records ORDER BY id DESC LIMIT $1',
          [Math.min(50, totalRows)],
        );
      } else {
        await this.pool.query('SELECT * FROM bench_records WHERE key = $1', [
          `key-nonexistent-${i}`,
        ]);
      }
    }
    const totalMs = performance.now() - start;

    return {
      operation: 'read',
      count,
      totalMs: Math.round(totalMs * 100) / 100,
      avgMs: Math.round((totalMs / count) * 1000) / 1000,
      opsPerSec: Math.round((count / totalMs) * 1000),
    };
  }

  async getRowCount(): Promise<number> {
    const res = await this.pool.query(
      'SELECT COUNT(*) as cnt FROM bench_records',
    );
    return parseInt(res.rows[0].cnt);
  }

  async reset(): Promise<void> {
    await this.pool.query('TRUNCATE bench_records RESTART IDENTITY');
  }
}
