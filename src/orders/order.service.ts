import { Inject, Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  Order,
  OrderEventPayload,
  OrderEventRecord,
  OrderEventType,
  OrderItem,
  OrderStatus,
} from './order-events';
import {
  DuplicateEventSummary,
  ORDER_STORE,
  OrderStore,
} from './order-store.interface';
import { OrderPublisher, PublishResult } from './order-publisher';

export interface CreateOrderInput {
  userId?: string;
  items?: OrderItem[];
}

export interface CreateOrderResult {
  order: Order;
  dispatch: PublishResult;
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
    const dispatch = await this.publisher.publish(
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
