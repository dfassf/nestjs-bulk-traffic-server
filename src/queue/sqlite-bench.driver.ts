import * as path from 'path';
import { BenchDriver, BenchResult } from './bench-driver.interface';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const Database = require('better-sqlite3');

export class SqliteBenchDriver implements BenchDriver {
  private db: any;
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
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_bench_key ON bench_records(key)`);
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
        insert.run(`key-${Date.now()}-${i}`, JSON.stringify({ i, data: 'x'.repeat(100) }), Date.now());
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
    const totalRows = this.db.prepare('SELECT COUNT(*) as cnt FROM bench_records').get();
    if (totalRows.cnt === 0) {
      return { operation: 'read', count: 0, totalMs: 0, avgMs: 0, opsPerSec: 0 };
    }

    const selectByKey = this.db.prepare('SELECT * FROM bench_records WHERE key = ?');
    const selectRange = this.db.prepare('SELECT * FROM bench_records ORDER BY id DESC LIMIT ?');

    const start = performance.now();
    for (let i = 0; i < count; i++) {
      if (i % 2 === 0) {
        selectRange.all(Math.min(50, totalRows.cnt));
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
    return this.db.prepare('SELECT COUNT(*) as cnt FROM bench_records').get().cnt;
  }

  async reset(): Promise<void> {
    this.db.exec('DELETE FROM bench_records');
    this.db.exec('VACUUM');
  }
}
