import { BenchDriver, BenchResult } from './bench-driver.interface';
import { Pool } from 'pg';

export class PgBenchDriver implements BenchDriver {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.BENCH_PG_HOST || 'localhost',
      port: parseInt(process.env.BENCH_PG_PORT || '5432'),
      database: process.env.BENCH_PG_DATABASE || 'bench',
      user: process.env.BENCH_PG_USER || 'bench',
      password: process.env.BENCH_PG_PASSWORD || 'bench',
      max: parseInt(process.env.BENCH_PG_POOL_SIZE || '10'),
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
      const stmt = 'INSERT INTO bench_records (key, value, created_at) VALUES ($1, $2, $3)';
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
    const res = await this.pool.query('SELECT COUNT(*) as cnt FROM bench_records');
    const totalRows = parseInt(res.rows[0].cnt);
    if (totalRows === 0) {
      return { operation: 'read', count: 0, totalMs: 0, avgMs: 0, opsPerSec: 0 };
    }

    const start = performance.now();
    for (let i = 0; i < count; i++) {
      if (i % 2 === 0) {
        await this.pool.query(
          'SELECT * FROM bench_records ORDER BY id DESC LIMIT $1',
          [Math.min(50, totalRows)],
        );
      } else {
        await this.pool.query(
          'SELECT * FROM bench_records WHERE key = $1',
          [`key-nonexistent-${i}`],
        );
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
    const res = await this.pool.query('SELECT COUNT(*) as cnt FROM bench_records');
    return parseInt(res.rows[0].cnt);
  }

  async reset(): Promise<void> {
    await this.pool.query('TRUNCATE bench_records RESTART IDENTITY');
  }
}
