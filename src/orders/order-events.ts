/**
 * 주문 처리 흐름의 이벤트 정의.
 *
 * 카프카 실험용 소재다. 실제 결제 연동이나 재고 정합성이 목적이 아니라,
 * 카프카가 어떤 상황에서 어떻게 동작하는지 드러낼 재료가 필요해서 둔다.
 * 자세한 배경은 docs/kafka-lab-plan.md 참고.
 */

/** 주문 한 건이 거치는 단계. */
export enum OrderEventType {
  CREATED = 'orders.created',
  INVENTORY_RESERVED = 'inventory.reserved',
  PAYMENT_APPROVED = 'payments.approved',
  SHIPMENT_STARTED = 'shipments.started',
}

/**
 * 이벤트별 토픽.
 *
 * 이벤트 종류가 곧 토픽 이름이라 값이 같지만, 둘은 다른 개념이라 따로 둔다.
 * 나중에 한 토픽에 여러 이벤트를 싣거나 토픽 이름 규칙이 바뀌면 여기만 고친다.
 */
export const ORDER_TOPICS: Record<OrderEventType, string> = {
  [OrderEventType.CREATED]: 'orders.created',
  [OrderEventType.INVENTORY_RESERVED]: 'inventory.reserved',
  [OrderEventType.PAYMENT_APPROVED]: 'payments.approved',
  [OrderEventType.SHIPMENT_STARTED]: 'shipments.started',
};

/** 주문 상태. 이벤트가 처리되면서 이 순서로 넘어간다. */
export enum OrderStatus {
  CREATED = 'created',
  INVENTORY_RESERVED = 'inventory_reserved',
  PAYMENT_APPROVED = 'payment_approved',
  SHIPPED = 'shipped',
  FAILED = 'failed',
}

export interface OrderItem {
  productId: string;
  quantity: number;
  unitPrice: number;
}

export interface Order {
  orderId: string;
  userId: string;
  amount: number;
  items: OrderItem[];
  status: OrderStatus;
  createdAt: number;
  updatedAt: number;
}

/** 카프카로 실려 나가는 이벤트 본문. */
export interface OrderEventPayload {
  eventType: OrderEventType;
  orderId: string;
  userId: string;
  amount: number;
  items: OrderItem[];
  /** 이벤트가 만들어진 시각. 카프카가 찍는 시각과 구분하려고 따로 담는다. */
  emittedAt: number;
}

/**
 * 이벤트가 실제로 소비된 기록.
 *
 * 파티션·오프셋을 남겨야 "몇 번 중복 처리됐나", "어떤 순서로 왔나" 를
 * 나중에 쿼리로 확인할 수 있다. 실험 관측의 핵심이다.
 */
export interface OrderEventRecord {
  id?: number;
  orderId: string;
  eventType: OrderEventType;
  topic: string;
  partition: number;
  offset: string;
  /** 어느 컨슈머가 처리했는지. 파티션 분배를 볼 때 쓴다. */
  consumerId: string;
  consumedAt: number;
}

/**
 * 메시지 키를 고른다.
 *
 * 같은 키는 같은 파티션으로 가서 순서가 보장된다.
 * 주문 흐름은 orderId 로 묶어야 "결제 전에 배송이 처리되는" 역전을 막는다.
 * 재고만 productId 인 이유는 재고 경합이 상품 단위로 일어나기 때문이다.
 */
export function pickEventKey(payload: OrderEventPayload): string {
  if (payload.eventType === OrderEventType.INVENTORY_RESERVED) {
    const firstItem = payload.items[0];
    if (!firstItem) {
      throw new Error(
        `재고 이벤트에 품목이 없습니다. orderId=${payload.orderId}. ` +
          '품목 없이 발행하면 파티션 배정 근거가 사라져 순서 보장이 깨집니다.',
      );
    }
    return firstItem.productId;
  }
  return payload.orderId;
}

/** 이벤트 종류에 대응하는 토픽. */
export function topicFor(eventType: OrderEventType): string {
  const topic = ORDER_TOPICS[eventType];
  if (!topic) {
    throw new Error(`토픽이 정의되지 않은 이벤트 종류입니다: ${eventType}`);
  }
  return topic;
}

/** 이벤트 처리 후 넘어갈 주문 상태. */
export function statusAfter(eventType: OrderEventType): OrderStatus {
  switch (eventType) {
    case OrderEventType.CREATED:
      return OrderStatus.CREATED;
    case OrderEventType.INVENTORY_RESERVED:
      return OrderStatus.INVENTORY_RESERVED;
    case OrderEventType.PAYMENT_APPROVED:
      return OrderStatus.PAYMENT_APPROVED;
    case OrderEventType.SHIPMENT_STARTED:
      return OrderStatus.SHIPPED;
    default: {
      // 새 이벤트를 추가하면 여기서 컴파일이 깨진다. 조용히 특정 분기로 흐르지 않게.
      const unreachable: never = eventType;
      throw new Error(`알 수 없는 이벤트 종류: ${String(unreachable)}`);
    }
  }
}
