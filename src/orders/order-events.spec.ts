import {
  OrderEventPayload,
  OrderEventType,
  OrderStatus,
  pickEventKey,
  statusAfter,
  topicFor,
} from './order-events';

function buildPayload(
  overrides: Partial<OrderEventPayload> = {},
): OrderEventPayload {
  return {
    eventType: OrderEventType.CREATED,
    orderId: 'ord-1',
    userId: 'user-1',
    amount: 10000,
    items: [{ productId: 'prod-A', quantity: 2, unitPrice: 5000 }],
    emittedAt: 1_700_000_000_000,
    ...overrides,
  };
}

describe('topicFor', () => {
  it.each([
    [OrderEventType.CREATED, 'orders.created'],
    [OrderEventType.INVENTORY_RESERVED, 'inventory.reserved'],
    [OrderEventType.PAYMENT_APPROVED, 'payments.approved'],
    [OrderEventType.SHIPMENT_STARTED, 'shipments.started'],
  ])('%s -> %s', (eventType, topic) => {
    expect(topicFor(eventType)).toBe(topic);
  });

  it('정의되지 않은 이벤트 종류는 기본 토픽으로 넘기지 않고 예외를 던진다', () => {
    expect(() => topicFor('orders.unknown' as OrderEventType)).toThrow(
      /토픽이 정의되지 않은/,
    );
  });
});

describe('pickEventKey', () => {
  // 같은 키는 같은 파티션으로 간다. 주문 흐름을 orderId 로 묶어야
  // 결제 전에 배송이 처리되는 순서 역전을 막는다.
  it.each([
    OrderEventType.CREATED,
    OrderEventType.PAYMENT_APPROVED,
    OrderEventType.SHIPMENT_STARTED,
  ])('%s 는 orderId 를 키로 쓴다', (eventType) => {
    expect(pickEventKey(buildPayload({ eventType }))).toBe('ord-1');
  });

  // 재고는 상품 단위로 경합하므로 productId 로 묶는다.
  it('재고 이벤트는 첫 품목의 productId 를 키로 쓴다', () => {
    const payload = buildPayload({
      eventType: OrderEventType.INVENTORY_RESERVED,
      items: [
        { productId: 'prod-X', quantity: 1, unitPrice: 100 },
        { productId: 'prod-Y', quantity: 1, unitPrice: 200 },
      ],
    });
    expect(pickEventKey(payload)).toBe('prod-X');
  });

  it('같은 주문의 이벤트는 같은 키를 낸다 (순서 보장의 근거)', () => {
    const created = buildPayload({ eventType: OrderEventType.CREATED });
    const paid = buildPayload({ eventType: OrderEventType.PAYMENT_APPROVED });
    const shipped = buildPayload({
      eventType: OrderEventType.SHIPMENT_STARTED,
    });

    expect(pickEventKey(created)).toBe(pickEventKey(paid));
    expect(pickEventKey(paid)).toBe(pickEventKey(shipped));
  });

  // 키가 없으면 파티션이 흩어져 순서가 깨진다. 빈 문자열로 때우면 그게 숨는다.
  it('재고 이벤트에 품목이 없으면 예외를 던진다', () => {
    const payload = buildPayload({
      eventType: OrderEventType.INVENTORY_RESERVED,
      items: [],
    });
    expect(() => pickEventKey(payload)).toThrow(/품목이 없습니다/);
  });
});

describe('statusAfter', () => {
  it.each([
    [OrderEventType.CREATED, OrderStatus.CREATED],
    [OrderEventType.INVENTORY_RESERVED, OrderStatus.INVENTORY_RESERVED],
    [OrderEventType.PAYMENT_APPROVED, OrderStatus.PAYMENT_APPROVED],
    [OrderEventType.SHIPMENT_STARTED, OrderStatus.SHIPPED],
  ])('%s 처리 후 상태는 %s', (eventType, status) => {
    expect(statusAfter(eventType)).toBe(status);
  });

  it('알 수 없는 이벤트는 특정 상태로 흘려보내지 않고 예외를 던진다', () => {
    expect(() => statusAfter('orders.unknown' as OrderEventType)).toThrow(
      /알 수 없는 이벤트/,
    );
  });
});
