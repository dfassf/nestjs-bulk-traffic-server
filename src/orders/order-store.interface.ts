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

  /**
   * 컨슈머 그룹별 소비 건수.
   *
   * 한 이벤트를 여러 그룹이 각자 소비하는지 보려면 그룹으로 갈라 세야 한다.
   * 전체 건수만 보면 세 그룹이 나눠 가진 것과 각자 전량 받은 것이 구분되지 않는다.
   */
  countByGroup(): Promise<GroupConsumptionSummary[]>;

  countOrders(): Promise<number>;
  countEvents(): Promise<number>;

  /** 실험을 새로 시작할 때 비운다. */
  reset(): Promise<void>;
}

/** 한 컨슈머 그룹이 소비한 결과. */
export interface GroupConsumptionSummary {
  /** 컨슈머 그룹 이름. consumer_id 의 '그룹/프로세스-순번' 에서 앞부분. */
  groupId: string;
  /** 그 그룹이 처리한 이벤트 건수. */
  count: number;
  /** 그 그룹에서 일한 컨슈머 수. 파티션 분배를 볼 때 쓴다. */
  consumers: number;
}

export interface DuplicateEventSummary {
  orderId: string;
  eventType: OrderEventType;
  count: number;
}

export const ORDER_STORE = Symbol('ORDER_STORE');
