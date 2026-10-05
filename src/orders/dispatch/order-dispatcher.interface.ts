import { OrderEventPayload } from '../order-events';

/**
 * 주문 이벤트를 어디론가 내보내는 통로.
 *
 * 카프카로 보낼지 프로세스 안의 큐에 쌓을지를 이 인터페이스 뒤에 숨긴다.
 * 주문 서비스는 "보냈다" 만 알고, 무엇으로 보냈는지는 모른다.
 *
 * 둘을 나란히 재려면 보내는 수단만 바꿀 수 있어야 한다.
 * 수단마다 주문 생성 코드를 따로 두면 비교 대상이 흐려진다.
 */
export interface OrderDispatcher {
  /** 이 통로가 지금 쓸 수 있는 상태인지. */
  isReady(): boolean;

  /** 어떤 수단인지 보여주는 이름. 측정 결과에 조건으로 함께 남긴다. */
  readonly name: string;

  dispatch(payload: OrderEventPayload): Promise<DispatchResult>;
}

/**
 * 내보낸 결과.
 *
 * 카프카는 파티션·오프셋이 있고 프로세스 안의 큐는 없다.
 * 없는 값을 0 으로 채우면 "0번 파티션" 과 구분되지 않으므로 null 로 둔다.
 */
export interface DispatchResult {
  /** 어디로 보냈는지(토픽 이름 또는 큐 이름). */
  destination: string;
  /** 카프카만 해당. 프로세스 안의 큐는 null. */
  partition: number | null;
  /** 카프카만 해당. 프로세스 안의 큐는 null. */
  offset: string | null;
  /** 순서를 묶는 키. 키 없이 보내는 실험에서는 null. */
  key: string | null;
}

export const ORDER_DISPATCHER = Symbol('ORDER_DISPATCHER');
