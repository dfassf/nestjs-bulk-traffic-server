import * as path from 'path';
import Database from 'better-sqlite3';
import { BenchDriver, BenchResult } from './bench-driver.interface';

/** COUNT(*) 한 개만 돌려주는 집계 결과. */
interface CountRow {
  cnt: number;
}

export class SqliteBenchDriver implements BenchDriver {
  private db: Database.Database;
  private readonly dbPath: string;

  constructor() {
    this.dbPath = path.resolve(process.env.BENCH_DB_PATH || '.bench.sqlite');
  }

  async init(): Promise<void> {
    this.db = new Database(this.dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS bench_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_bench_key ON bench_records(key)`,
    );
  }

  async destroy(): Promise<void> {
    this.db?.close();
  }

  async benchWrite(count: number): Promise<BenchResult> {
    const insert = this.db.prepare(
      'INSERT INTO bench_records (key, value, created_at) VALUES (?, ?, ?)',
    );

    const start = performance.now();
    const batchInsert = this.db.transaction(() => {
      for (let i = 0; i < count; i++) {
        insert.run(
          `key-${Date.now()}-${i}`,
          JSON.stringify({ i, data: 'x'.repeat(100) }),
          Date.now(),
        );
      }
    });
    batchInsert();
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
    const totalRows = this.readRowCount();
    if (totalRows === 0) {
      return {
        operation: 'read',
        count: 0,
        totalMs: 0,
        avgMs: 0,
        opsPerSec: 0,
      };
    }

    const selectByKey = this.db.prepare(
      'SELECT * FROM bench_records WHERE key = ?',
    );
    const selectRange = this.db.prepare(
      'SELECT * FROM bench_records ORDER BY id DESC LIMIT ?',
    );

    const start = performance.now();
    for (let i = 0; i < count; i++) {
      if (i % 2 === 0) {
        selectRange.all(Math.min(50, totalRows));
      } else {
        selectByKey.all(`key-nonexistent-${i}`);
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
    return this.readRowCount();
  }

  /**
   * 적재된 행 수를 읽는다.
   *
   * COUNT 는 항상 한 행을 돌려주므로 결과가 비면 쿼리가 잘못된 것이다.
   * 0 으로 대신하면 "정말 0건" 과 "쿼리가 틀림" 이 구분되지 않는다.
   */
  private readRowCount(): number {
    const row = this.db
      .prepare<[], CountRow>('SELECT COUNT(*) as cnt FROM bench_records')
      .get();
    if (!row) {
      throw new Error('벤치 테이블 건수를 읽지 못했습니다.');
    }
    return row.cnt;
  }

  async reset(): Promise<void> {
    this.db.exec('DELETE FROM bench_records');
    this.db.exec('VACUUM');
  }
}
