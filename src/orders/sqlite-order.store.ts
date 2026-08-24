import * as path from 'path';
import Database from 'better-sqlite3';
import {
  Order,
  OrderEventRecord,
  OrderEventType,
  OrderItem,
  OrderStatus,
} from './order-events';
import { DuplicateEventSummary, OrderStore } from './order-store.interface';

interface OrderRow {
  order_id: string;
  user_id: string;
  amount: number;
  items: string;
  status: string;
  created_at: number;
  updated_at: number;
}

interface EventRow {
  id: number;
  order_id: string;
  event_type: string;
  topic: string;
  partition: number;
  offset: string;
  consumer_id: string;
  consumed_at: number;
}

/** 같은 주문·이벤트가 몇 번 처리됐는지 세는 집계 결과. */
interface DuplicateRow {
  order_id: string;
  event_type: string;
  cnt: number;
}

/** COUNT(*) 한 개만 돌려주는 집계 결과. */
interface CountRow {
  cnt: number;
}

/**
 * 파일이 아니라 메모리에 두라는 뜻의 특수 경로.
 *
 * better-sqlite3 는 이 값을 그대로 넘겨야 메모리 DB 가 된다.
 * 경로로 취급해 절대경로로 바꾸면 ':memory:' 라는 이름의 파일이 실제로 생기고,
 * 테스트가 메모리에서 도는 줄 알았는데 파일에 쌓이면서 테스트끼리 서로 간섭한다.
 */
const IN_MEMORY_PATH = ':memory:';

/**
 * SQLite 기반 주문 저장소.
 *
 * 카프카 실험 관측용이라 스키마가 단순하다.
 * 벤치마크 DB 와 파일을 분리해서 실험을 초기화해도 벤치 결과가 안 날아가게 한다.
 */
export class SqliteOrderStore implements OrderStore {
  private db: Database.Database;
  private readonly dbPath: string;

  constructor(dbPath?: string) {
    const raw = dbPath ?? process.env.ORDER_DB_PATH ?? '.orders.sqlite';
    this.dbPath = raw === IN_MEMORY_PATH ? IN_MEMORY_PATH : path.resolve(raw);
  }

  /** 메모리 DB 인지. 파일에만 의미가 있는 설정을 건너뛸 때 쓴다. */
  isInMemory(): boolean {
    return this.dbPath === IN_MEMORY_PATH;
  }

  async init(): Promise<void> {
    this.db = new Database(this.dbPath);
    // WAL 은 별도 파일(-wal·-shm)에 쓰는 방식이라 메모리 DB 에서는 의미가 없다.
    if (!this.isInMemory()) {
      this.db.pragma('journal_mode = WAL');
    }

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        amount INTEGER NOT NULL,
        items TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);

    // 중복 처리를 관측하는 게 목적이라 유니크 제약을 두지 않는다.
    // 같은 이벤트가 두 번 오면 두 행이 쌓여야 실험에서 그게 보인다.
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS order_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        topic TEXT NOT NULL,
        partition INTEGER NOT NULL,
        offset TEXT NOT NULL,
        consumer_id TEXT NOT NULL,
        consumed_at INTEGER NOT NULL
      )
    `);

    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_events_order ON order_events(order_id, event_type)`,
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_events_consumed ON order_events(consumed_at)`,
    );
  }

  async destroy(): Promise<void> {
    this.db?.close();
  }

  async saveOrder(order: Order): Promise<void> {
    this.db
      .prepare(
        `INSERT INTO orders (order_id, user_id, amount, items, status, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        order.orderId,
        order.userId,
        order.amount,
        JSON.stringify(order.items),
        order.status,
        order.createdAt,
        order.updatedAt,
      );
  }

  async findOrder(orderId: string): Promise<Order | null> {
    const row = this.db
      .prepare<[string], OrderRow>('SELECT * FROM orders WHERE order_id = ?')
      .get(orderId);

    return row ? this.toOrder(row) : null;
  }

  async updateStatus(orderId: string, status: OrderStatus): Promise<void> {
    const result = this.db
      .prepare(
        'UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?',
      )
      .run(status, Date.now(), orderId);

    // 없는 주문을 갱신하려 한 건 호출 측 버그다. 조용히 넘기면 상태가 어긋난 걸 모른다.
    if (result.changes === 0) {
      throw new Error(
        `주문을 찾을 수 없어 상태를 바꾸지 못했습니다: ${orderId}`,
      );
    }
  }

  async recordEvent(record: OrderEventRecord): Promise<void> {
    this.db
      .prepare(
        `INSERT INTO order_events
         (order_id, event_type, topic, partition, offset, consumer_id, consumed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        record.orderId,
        record.eventType,
        record.topic,
        record.partition,
        record.offset,
        record.consumerId,
        record.consumedAt,
      );
  }

  async findEvents(orderId?: string, limit = 200): Promise<OrderEventRecord[]> {
    const rows: EventRow[] = orderId
      ? this.db
          .prepare<
            [string, number],
            EventRow
          >('SELECT * FROM order_events WHERE order_id = ? ORDER BY consumed_at ASC, id ASC LIMIT ?')
          .all(orderId, limit)
      : this.db
          .prepare<
            [number],
            EventRow
          >('SELECT * FROM order_events ORDER BY id DESC LIMIT ?')
          .all(limit);

    return rows.map((row) => this.toEventRecord(row));
  }

  async findDuplicates(): Promise<DuplicateEventSummary[]> {
    const rows = this.db
      .prepare<[], DuplicateRow>(
        `SELECT order_id, event_type, COUNT(*) AS cnt
         FROM order_events
         GROUP BY order_id, event_type
         HAVING COUNT(*) > 1
         ORDER BY cnt DESC`,
      )
      .all();

    return rows.map((row) => ({
      orderId: row.order_id,
      eventType: row.event_type as OrderEventType,
      count: row.cnt,
    }));
  }

  async countOrders(): Promise<number> {
    return this.count('SELECT COUNT(*) AS cnt FROM orders');
  }

  async countEvents(): Promise<number> {
    return this.count('SELECT COUNT(*) AS cnt FROM order_events');
  }

  /**
   * COUNT(*) 한 개를 읽는다.
   *
   * COUNT 는 항상 한 행을 돌려주므로 결과가 비면 쿼리가 잘못된 것이다.
   * 0 으로 대신하면 "정말 0건" 과 "쿼리가 틀림" 이 구분되지 않는다.
   */
  private count(sql: string): number {
    const row = this.db.prepare<[], CountRow>(sql).get();
    if (!row) {
      throw new Error(`건수를 읽지 못했습니다: ${sql}`);
    }
    return row.cnt;
  }

  async reset(): Promise<void> {
    this.db.exec('DELETE FROM order_events');
    this.db.exec('DELETE FROM orders');
  }

  private toOrder(row: OrderRow): Order {
    return {
      orderId: row.order_id,
      userId: row.user_id,
      amount: row.amount,
      items: this.parseItems(row.items, row.order_id),
      status: row.status as OrderStatus,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private parseItems(raw: string, orderId: string): OrderItem[] {
    try {
      return JSON.parse(raw);
    } catch (error) {
      // 빈 배열로 덮으면 품목이 원래 없었던 것처럼 보인다. 깨진 건 깨진 대로 알린다.
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(
        `주문 품목을 읽지 못했습니다. orderId=${orderId}: ${reason}`,
      );
    }
  }

  private toEventRecord(row: EventRow): OrderEventRecord {
    return {
      id: row.id,
      orderId: row.order_id,
      eventType: row.event_type as OrderEventType,
      topic: row.topic,
      partition: row.partition,
      offset: row.offset,
      consumerId: row.consumer_id,
      consumedAt: row.consumed_at,
    };
  }
}
