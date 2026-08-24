import {
  Order,
  OrderEventRecord,
  OrderEventType,
  OrderStatus,
} from './order-events';

/**
 * 주문·이벤트 저장소.
 *
 * 기존 BenchDriver 는 벤치마크 전용(benchWrite·benchRead)이라 주문 도메인에 맞지 않아
 * 따로 둔다. 구현은 SQLite 를 재사용한다.
 */
export interface OrderStore {
  init(): Promise<void>;
  destroy(): Promise<void>;

  saveOrder(order: Order): Promise<void>;
  findOrder(orderId: string): Promise<Order | null>;
  updateStatus(orderId: string, status: OrderStatus): Promise<void>;

  /**
   * 이벤트 소비 기록을 남긴다.
   *
   * 중복 처리를 관측하는 게 목적이라 같은 이벤트가 두 번 와도 두 행이 쌓인다.
   * 막지 않는다. 막으면 실험에서 보려는 현상이 사라진다.
   */
  recordEvent(record: OrderEventRecord): Promise<void>;

  findEvents(orderId?: string, limit?: number): Promise<OrderEventRecord[]>;

  /** 같은 주문·이벤트가 두 번 이상 처리된 건. 중복 실험의 관측 지표. */
  findDuplicates(): Promise<DuplicateEventSummary[]>;

  countOrders(): Promise<number>;
  countEvents(): Promise<number>;

  /** 실험을 새로 시작할 때 비운다. */
  reset(): Promise<void>;
}

export interface DuplicateEventSummary {
  orderId: string;
  eventType: OrderEventType;
  count: number;
}

export const ORDER_STORE = Symbol('ORDER_STORE');
