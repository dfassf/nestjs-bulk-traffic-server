import { Inject, Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  Order,
  OrderEventPayload,
  OrderEventRecord,
  OrderEventType,
  OrderItem,
  OrderStatus,
  statusAfter,
} from './order-events';
import {
  DuplicateEventSummary,
  GroupConsumptionSummary,
  ORDER_STORE,
  OrderStore,
} from './order-store.interface';
import { OrderPublisher } from './order-publisher';
import {
  DispatchResult,
  ORDER_DISPATCHER,
  OrderDispatcher,
} from './dispatch/order-dispatcher.interface';

export interface CreateOrderInput {
  userId?: string;
  items?: OrderItem[];
}

export interface CreateOrderResult {
  order: Order;
  dispatch: DispatchResult;
}

/**
 * 주문 생성 뒤에 이어지는 단계들.
 *
 * CREATED 는 주문을 만들 때 이미 발행되므로 여기서 제외한다.
 * 포함하면 생성 이벤트가 두 번 나가 중복 집계가 틀어진다.
 */
const LIFECYCLE_EVENT_TYPES = [
  OrderEventType.INVENTORY_RESERVED,
  OrderEventType.PAYMENT_APPROVED,
  OrderEventType.SHIPMENT_STARTED,
] as const;

/** 흐름 한 단계를 내보낸 결과. */
export interface LifecycleStep {
  eventType: OrderEventType;
  destination: string;
  partition: number | null;
  offset: string | null;
  key: string | null;
}

export interface LifecycleResult {
  orderId: string;
  steps: LifecycleStep[];
}

/**
 * 같은 주문의 단계들이 몇 개의 파티션으로 흩어졌는지.
 *
 * 카프카는 같은 토픽·같은 파티션 안에서만 순서를 지킨다. 한 주문의 단계가
 * 여러 파티션으로 갈라지면 받는 쪽에서 순서가 뒤바뀔 수 있다.
 */
export interface ScatterSummary {
  /** 단계가 한 파티션에 모인 주문 수. */
  singlePartition: number;
  /** 단계가 여러 파티션으로 흩어진 주문 수. */
  multiPartition: number;
  /** 주문당 평균 파티션 수. 1 이면 전부 모였다는 뜻. */
  avgPartitionsPerOrder: number | null;
}

/**
 * 흩어짐을 센다.
 *
 * 파티션을 모르는 통로(프로세스 안의 큐)는 null 을 주므로 셀 대상이 아니다.
 * 0 으로 치면 "0번 파티션에 모였다" 로 읽혀 결과가 뒤집힌다.
 */
function summarizeScatter(lifecycles: LifecycleResult[]): ScatterSummary {
  let single = 0;
  let multi = 0;
  let partitionTotal = 0;
  let counted = 0;

  for (const lifecycle of lifecycles) {
    const partitions = new Set(
      lifecycle.steps
        .map((step) => step.partition)
        .filter((p): p is number => p !== null),
    );
    if (partitions.size === 0) continue;

    counted++;
    partitionTotal += partitions.size;
    if (partitions.size === 1) single++;
    else multi++;
  }

  return {
    singlePartition: single,
    multiPartition: multi,
    // 표본이 없으면 0 이 아니라 null. 0 은 "파티션이 없다" 로 읽힌다.
    avgPartitionsPerOrder: counted > 0 ? partitionTotal / counted : null,
  };
}

export interface LifecycleBulkResult {
  requested: number;
  completed: number;
  failed: number;
  elapsedMs: number;
  orderIds: string[];
  errors: string[];
  /** 같은 주문의 단계가 몇 갈래로 흩어졌는지. 키 효과를 보는 지표다. */
  scatter: ScatterSummary;
  /** 눈으로 확인할 샘플 몇 건. */
  samples: LifecycleResult[];
}

export interface BulkOrderResult {
  requested: number;
  created: number;
  failed: number;
  elapsedMs: number;
  /** 파티션별로 몇 건이 갔는지. 키 라우팅이 고르게 퍼지는지 볼 때 쓴다. */
  partitionCounts: Record<number, number>;
  errors: string[];
}

const DEFAULT_ITEM: OrderItem = {
  productId: 'prod-default',
  quantity: 1,
  unitPrice: 10000,
};

@Injectable()
export class OrderService {
  private readonly logger = new Logger(OrderService.name);

  constructor(
    @Inject(ORDER_STORE) private readonly store: OrderStore,
    @Inject(ORDER_DISPATCHER)
    private readonly dispatcher: OrderDispatcher,
    // 카프카 전용 설정(멱등성·acks·키 사용)을 현황에 보여주기 위해 함께 받는다.
    // 내보내는 일은 dispatcher 가 하고, 이쪽은 설정 조회용이다.
    private readonly publisher: OrderPublisher,
  ) {}

  async createOrder(input: CreateOrderInput): Promise<CreateOrderResult> {
    const now = Date.now();
    const items = input.items?.length ? input.items : [DEFAULT_ITEM];
    const order: Order = {
      orderId: `ord-${randomUUID()}`,
      userId: input.userId ?? `user-${Math.floor(Math.random() * 1000)}`,
      amount: items.reduce(
        (sum, item) => sum + item.quantity * item.unitPrice,
        0,
      ),
      items,
      status: OrderStatus.CREATED,
      createdAt: now,
      updatedAt: now,
    };

    await this.store.saveOrder(order);

    // 저장이 끝난 뒤 발행한다. 발행이 실패하면 주문은 남고 이벤트만 없는 상태가 되는데,
    // 그 어긋남 자체가 카프카 실험에서 볼 거리다(발행 실패 시 어떻게 복구하는가).
    const dispatch = await this.dispatcher.dispatch(
      this.toPayload(order, OrderEventType.CREATED),
    );

    return { order, dispatch };
  }

  /**
   * 대량 생성. 부하를 걸어 Lag·파티션 분배를 관찰할 때 쓴다.
   *
   * 개별 실패를 관용하되 건수를 세서 보고한다. 조용히 넘기면
   * 100건 중 40건이 실패해도 "완료"로 보인다.
   */
  async createBulk(count: number, delayMs = 0): Promise<BulkOrderResult> {
    const startedAt = Date.now();
    const partitionCounts: Record<number, number> = {};
    const errors: string[] = [];
    let created = 0;

    for (let i = 0; i < count; i++) {
      try {
        const { dispatch } = await this.createOrder({});
        created++;
        partitionCounts[dispatch.partition] =
          (partitionCounts[dispatch.partition] ?? 0) + 1;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        // 앞의 몇 건만 남긴다. 전부 담으면 응답이 실패 메시지로 뒤덮인다.
        if (errors.length < 5) errors.push(message);
      }

      if (delayMs > 0) await this.sleep(delayMs);
    }

    const failed = count - created;
    if (failed > 0) {
      this.logger.warn(`대량 주문 생성 ${failed}/${count}건 실패`);
    }

    return {
      requested: count,
      created,
      failed,
      elapsedMs: Date.now() - startedAt,
      partitionCounts,
      errors,
    };
  }

  async findOrder(orderId: string): Promise<Order | null> {
    return this.store.findOrder(orderId);
  }

  async findEvents(
    orderId?: string,
    limit?: number,
  ): Promise<OrderEventRecord[]> {
    return this.store.findEvents(orderId, limit);
  }

  async findDuplicates(): Promise<DuplicateEventSummary[]> {
    return this.store.findDuplicates();
  }

  /** 컨슈머 그룹별 소비 건수. 여러 그룹이 각자 전량 받는지 볼 때 쓴다. */
  async countByGroup(): Promise<GroupConsumptionSummary[]> {
    return this.store.countByGroup();
  }

  async getStats() {
    const [orderCount, eventCount, duplicates] = await Promise.all([
      this.store.countOrders(),
      this.store.countEvents(),
      this.store.findDuplicates(),
    ]);

    return {
      orderCount,
      eventCount,
      duplicateGroups: duplicates.length,
      // 중복 처리된 총 건수(원본 1건을 뺀 초과분)
      duplicateExtra: duplicates.reduce((sum, d) => sum + (d.count - 1), 0),
      // 어떤 수단으로 내보내는 중인지. 측정 결과를 읽을 때 조건이 함께 보여야 한다.
      dispatcher: {
        name: this.dispatcher.name,
        ready: this.dispatcher.isReady(),
      },
      producer: {
        connected: this.publisher.isConnected(),
        idempotent: this.publisher.getConfig().idempotent,
        acks: this.publisher.getConfig().acks,
        keyEnabled: !this.publisher.getConfig().disableKey,
      },
    };
  }

  async reset(): Promise<void> {
    await this.store.reset();
    this.logger.log('주문·이벤트 기록을 비웠습니다.');
  }

  /**
   * 한 주문의 흐름을 단계 순서대로 내보낸다. 생성 → 재고 → 결제 → 배송.
   *
   * 실험용이다. 실제 재고 차감이나 결제 승인을 하지 않는다. 각 단계가
   * "일어났다" 는 이벤트만 순서대로 발행하고 주문 상태를 올린다.
   *
   * 단계 순서는 보낸 쪽에서 지킨다. 받는 쪽에서 그 순서가 유지되는지가
   * 키 설정에 달려 있고, 그걸 보는 것이 실험 6이다.
   */
  async dispatchLifecycle(orderId: string): Promise<LifecycleResult> {
    const order = await this.store.findOrder(orderId);
    if (!order) {
      throw new Error(`주문을 찾을 수 없습니다: ${orderId}`);
    }

    const steps: LifecycleStep[] = [];

    // CREATED 는 주문 생성 때 이미 나갔으므로 그 뒤 세 단계만 보낸다.
    // 여기서 다시 보내면 생성 이벤트가 두 번 발행돼 중복 집계가 틀어진다.
    for (const eventType of LIFECYCLE_EVENT_TYPES) {
      const dispatch = await this.dispatcher.dispatch(
        this.toPayload(order, eventType),
      );
      await this.store.updateStatus(orderId, statusAfter(eventType));

      steps.push({
        eventType,
        destination: dispatch.destination,
        partition: dispatch.partition,
        offset: dispatch.offset,
        key: dispatch.key,
      });
    }

    return { orderId, steps };
  }

  /**
   * 주문을 만들고 흐름 전체를 내보낸다. 실험 6에서 쓴다.
   *
   * 개별 주문의 실패를 관용하되 건수를 세서 보고한다.
   * 조용히 넘기면 10건 중 4건이 중간에 끊겨도 "완료" 로 보인다.
   */
  async createWithLifecycle(count: number): Promise<LifecycleBulkResult> {
    const startedAt = Date.now();
    const orderIds: string[] = [];
    const errors: string[] = [];
    const lifecycles: LifecycleResult[] = [];

    for (let i = 0; i < count; i++) {
      try {
        const { order } = await this.createOrder({});
        lifecycles.push(await this.dispatchLifecycle(order.orderId));
        orderIds.push(order.orderId);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (errors.length < 5) errors.push(message);
      }
    }

    const failed = count - orderIds.length;
    if (failed > 0) {
      this.logger.warn(`주문 흐름 발행 ${failed}/${count}건 실패`);
    }

    return {
      requested: count,
      completed: orderIds.length,
      failed,
      elapsedMs: Date.now() - startedAt,
      orderIds,
      errors,
      scatter: summarizeScatter(lifecycles),
      samples: lifecycles.slice(0, 3),
    };
  }

  private toPayload(
    order: Order,
    eventType: OrderEventType,
  ): OrderEventPayload {
    return {
      eventType,
      orderId: order.orderId,
      userId: order.userId,
      amount: order.amount,
      items: order.items,
      emittedAt: Date.now(),
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
