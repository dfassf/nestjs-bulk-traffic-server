import { Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { OrderEventPayload } from '../order-events';
import { ORDER_STORE, OrderStore } from '../order-store.interface';
import { readNonNegativeIntEnv } from '../../queue/utils/env';
import { DispatchResult, OrderDispatcher } from './order-dispatcher.interface';

/** 프로세스 안의 큐 이름. 카프카의 토픽 자리에 들어간다. */
const QUEUE_NAME = 'memory://orders';

/**
 * 프로세스 안의 큐로 내보내는 통로. 카프카와 나란히 재기 위한 비교 대상이다.
 *
 * 카프카와 다른 점이 셋 있고, 그게 바로 비교하려는 것이다.
 *
 * 1. 메시지가 이 프로세스의 메모리에만 있다. 프로세스가 죽으면 아직 처리하지
 *    않은 것은 같이 사라진다. 디스크에 남는 카프카와 달리 재개할 자리가 없다.
 * 2. 하나가 꺼내면 없어진다. 소비자를 여럿 두면 나눠 갖는다.
 *    카프카는 그룹마다 자기 위치를 따로 기억해서 각자 전량 받는다.
 * 3. 지나간 것을 다시 읽을 수 없다. 꺼낸 순간 큐에서 빠지므로 되감을 대상이 없다.
 *
 * 일부러 허술하게 만든 것이 아니라, 프로세스 메모리에 쌓는 방식의 성질이다.
 */
@Injectable()
export class MemoryOrderDispatcher implements OrderDispatcher, OnModuleDestroy {
  readonly name = 'memory';

  private readonly logger = new Logger(MemoryOrderDispatcher.name);
  private readonly queue: OrderEventPayload[] = [];

  /** 처리 루프가 돌고 있는지. 같은 루프를 두 번 돌리지 않으려고 둔다. */
  private draining = false;
  private stopped = false;

  /** 넣은 건수와 처리한 건수. 차이가 곧 아직 큐에 남은 양이다. */
  private enqueued = 0;
  private processed = 0;
  private failed = 0;

  /**
   * 한 건 처리에 걸리는 시간을 흉내낸다.
   *
   * 카프카 컨슈머의 CONSUMER_DELAY_MS 와 같은 성격이다. 없으면 처리가 너무
   * 빨라 큐에 쌓인 상태를 만들 수 없고, 그러면 "프로세스가 죽을 때 남은 것이
   * 사라진다" 를 재현할 수 없다. 같은 조건으로 비교하려면 양쪽에 있어야 한다.
   */
  private readonly processingDelayMs: number;

  constructor(@Inject(ORDER_STORE) private readonly store: OrderStore) {
    this.processingDelayMs = readNonNegativeIntEnv('MEMORY_CONSUMER_DELAY_MS');
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
  }

  isReady(): boolean {
    return !this.stopped;
  }

  /** 지금 상태. 유실 건수를 보려면 넣은 것과 처리한 것의 차이를 본다. */
  getStats(): {
    enqueued: number;
    processed: number;
    failed: number;
    pending: number;
  } {
    return {
      enqueued: this.enqueued,
      processed: this.processed,
      failed: this.failed,
      // 아직 큐에 남아 있는 양. 프로세스가 지금 죽으면 이만큼 사라진다.
      pending: this.queue.length,
    };
  }

  async dispatch(payload: OrderEventPayload): Promise<DispatchResult> {
    if (this.stopped) {
      throw new Error('프로세스 안의 큐가 이미 멈춰 있습니다.');
    }

    this.queue.push(payload);
    this.enqueued++;

    // 적재만 하고 바로 돌려준다. 카프카 발행도 브로커가 받으면 끝이고
    // 소비는 그 뒤에 따로 일어난다. 같은 조건으로 재야 한다.
    void this.drain();

    return {
      destination: QUEUE_NAME,
      // 파티션·오프셋이라는 개념이 없다. 0 으로 채우면 "0번" 과 구분되지 않는다.
      partition: null,
      offset: null,
      key: null,
    };
  }

  /**
   * 큐를 비운다.
   *
   * 꺼낸 메시지는 큐에서 사라진다. 처리하다 실패해도 돌려놓을 자리가 없다.
   * 카프카라면 커밋하지 않고 다시 읽으면 되는데 여기서는 그 건이 그대로 없어진다.
   */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    try {
      while (this.queue.length > 0 && !this.stopped) {
        const event = this.queue.shift();
        if (!event) continue;

        try {
          if (this.processingDelayMs > 0) {
            await new Promise((resolve) =>
              setTimeout(resolve, this.processingDelayMs),
            );
          }

          await this.store.recordEvent({
            orderId: event.orderId,
            eventType: event.eventType,
            topic: QUEUE_NAME,
            // 파티션이 없는 방식이라 -1 로 둔다. 0 으로 쓰면 카프카의
            // 0번 파티션과 섞여서 집계가 뒤엉킨다.
            partition: -1,
            offset: '',
            consumerId: `${this.name}/${process.pid}`,
            consumedAt: Date.now(),
          });
          this.processed++;
        } catch (error) {
          this.failed++;
          // 실패를 세서 드러낸다. 꺼낸 건 이미 큐에서 빠졌으므로 이 건은 사라진다.
          const reason = error instanceof Error ? error.message : String(error);
          this.logger.error(
            `처리 실패로 유실 orderId=${event.orderId}: ${reason}`,
          );
        }
      }
    } finally {
      this.draining = false;
    }
  }
}
