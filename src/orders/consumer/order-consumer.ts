import { Consumer, EachMessagePayload, Kafka, logLevel } from 'kafkajs';
import { ConsumerConfig } from './consumer-config';
import { OrderEventPayload, OrderEventType } from '../order-events';
import { OrderStore } from '../order-store.interface';

export interface ConsumerStats {
  consumerId: string;
  processed: number;
  failed: number;
  /** 파티션별 처리 건수. 분배가 고른지 볼 때 쓴다. */
  partitionCounts: Record<number, number>;
  startedAt: number;
  /**
   * 첫 건을 처리한 시각. 처리량을 낼 때 분모의 시작점이다.
   *
   * 컨슈머가 뜬 시각(startedAt)을 쓰면 메시지를 기다리며 논 시간까지
   * 분모에 들어가, 처리가 끝난 뒤에도 숫자가 계속 나빠진다.
   * Go 컨슈머와 같은 기준으로 재야 비교가 성립한다.
   */
  firstProcessedAt: number | null;
  lastProcessedAt: number | null;
  /** 한 건을 처리하는 데 걸린 시간(ms). 평균만 보면 느린 꼬리가 안 보인다. */
  latenciesMs: number[];
}

/**
 * 처리 성능 요약. 표본이 없으면 각 값이 null 이다(0 이 아니다).
 *
 * 0 으로 메우면 한 건도 처리 못 한 결과가 가장 빠른 것처럼 읽힌다.
 */
export interface ConsumerPerformance {
  processed: number;
  failed: number;
  avgMs: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
  throughputPerSec: number | null;
}

/**
 * 분위수. 위치는 올림으로 잡는다.
 *
 * 내림으로 잡으면 p95 가 실제보다 낮게 나와 느린 꼬리가 가려진다.
 * Go 컨슈머(internal/consumer/stats.go)와 같은 규칙이어야 비교가 성립한다.
 */
function percentileMs(values: number[], p: number): number | null {
  if (values.length === 0) return null;

  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    Math.max(Math.ceil((sorted.length * p) / 100) - 1, 0),
    sorted.length - 1,
  );
  return sorted[index];
}

/** 통계에서 성능 요약을 만든다. Go 쪽 Snapshot 과 같은 기준을 쓴다. */
export function summarizePerformance(
  stats: ConsumerStats,
): ConsumerPerformance {
  const { latenciesMs, firstProcessedAt, lastProcessedAt, processed } = stats;

  const avgMs =
    latenciesMs.length > 0
      ? latenciesMs.reduce((sum, ms) => sum + ms, 0) / latenciesMs.length
      : null;

  // 두 건 이상이라야 사이 간격이 생긴다. 한 건뿐이면 잰 구간이 0 이라 나눌 수 없다.
  let throughputPerSec: number | null = null;
  if (processed > 1 && firstProcessedAt !== null && lastProcessedAt !== null) {
    const elapsedMs = lastProcessedAt - firstProcessedAt;
    if (elapsedMs > 0) {
      throughputPerSec = (processed / elapsedMs) * 1000;
    }
  }

  return {
    processed,
    failed: stats.failed,
    avgMs,
    p50Ms: percentileMs(latenciesMs, 50),
    p95Ms: percentileMs(latenciesMs, 95),
    throughputPerSec,
  };
}

export interface ConsumerHooks {
  /**
   * 처리 건수가 crashAfter 에 도달해 이 컨슈머가 빠졌을 때.
   *
   * 이 컨슈머 하나만 빠진다. 프로세스를 끝내면 안 된다.
   * 같은 프로세스의 다른 컨슈머들이 커밋 직전에 함께 휩쓸려 죽어서,
   * 실제로는 없었을 중복까지 관측되기 때문이다(실험 숫자가 부풀려진다).
   */
  onCrashPoint?: (stats: ConsumerStats) => void;
}

/**
 * 주문 이벤트 컨슈머 한 개.
 *
 * 오프셋을 수동으로 커밋한다. 자동 커밋(기본값)은 주기적으로 백그라운드에서
 * 커밋해버려서 "언제 커밋되는지" 를 제어할 수 없다. 중복·유실 실험은
 * 커밋 시점이 전부라서 수동이어야 한다.
 */
export class OrderConsumer {
  private readonly consumer: Consumer;
  private readonly stats: ConsumerStats;
  private running = false;
  private crashed = false;

  constructor(
    private readonly config: ConsumerConfig,
    private readonly store: OrderStore,
    readonly consumerId: string,
    private readonly hooks: ConsumerHooks = {},
    kafka?: Kafka,
  ) {
    const client =
      kafka ??
      new Kafka({
        clientId: `${config.clientId}-${consumerId}`,
        brokers: config.brokers,
        logLevel: logLevel.WARN,
      });

    this.consumer = client.consumer({
      groupId: config.groupId,
      // 처리 지연을 크게 주면 기본 세션 타임아웃(30초) 안에 하트비트를 못 보내
      // 컨슈머가 그룹에서 쫓겨난다. 실험에서 일부러 느리게 만들 것이므로 넉넉히 둔다.
      sessionTimeout: 60000,
    });

    this.stats = {
      consumerId,
      processed: 0,
      failed: 0,
      partitionCounts: {},
      startedAt: Date.now(),
      firstProcessedAt: null,
      lastProcessedAt: null,
      latenciesMs: [],
    };
  }

  getStats(): Readonly<ConsumerStats> {
    return this.stats;
  }

  isRunning(): boolean {
    return this.running;
  }

  /** crashAfter 에 걸려 커밋 없이 빠졌는지. 정상 종료와 구분한다. */
  hasCrashed(): boolean {
    return this.crashed;
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    for (const topic of this.config.topics) {
      await this.consumer.subscribe({
        topic,
        fromBeginning: this.config.fromBeginning,
      });
    }

    await this.consumer.run({
      // 수동 커밋을 쓰려면 꺼야 한다. 켜두면 kafkajs 가 알아서 커밋해버린다.
      autoCommit: false,
      eachMessage: (payload) => this.handleMessage(payload),
    });

    this.running = true;
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    await this.consumer.disconnect();
    this.running = false;
  }

  /**
   * 커밋하지 않은 채 이 컨슈머만 그룹에서 빠진다. 프로세스는 그대로 둔다.
   *
   * 프로세스를 끝내면 같은 프로세스의 다른 컨슈머들이 커밋 직전에 함께 죽어,
   * 실제로는 없었을 중복까지 관측된다. 그러면 실험 숫자를 잘못 읽는다.
   *
   * disconnect() 는 그룹에서 빠졌다고 카프카에 알리므로, 남은 컨슈머들에게
   * 파티션이 곧바로 재분배된다. 방금 처리한 건은 커밋을 안 했으니
   * 재분배받은 컨슈머가 그 자리부터 다시 읽어 중복이 관측된다.
   *
   * 실제 프로세스 강제 종료(kill -9)와 다른 점: 그쪽은 카프카가 세션 만료를
   * 기다린 뒤에야 재분배한다. 여기서는 즉시 빠지므로 재분배가 더 빠르다.
   * 커밋을 놓쳐 중복이 생긴다는 관측 대상은 같다.
   */
  private async leaveWithoutCommit(): Promise<void> {
    await this.consumer.disconnect();
    this.running = false;
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;

    if (this.config.commitMode === 'before-process') {
      // 처리 전에 커밋한다. 여기서 죽으면 이 건은 영영 처리되지 않는다(유실).
      await this.commit(topic, partition, message.offset);
    }

    // 처리 시간만 잰다. 커밋은 빼야 두 런타임의 처리 성능을 비교할 수 있다.
    //
    // Date.now() 가 아니라 performance.now() 를 쓴다. 앞의 것은 1ms 해상도라
    // 1ms 보다 짧은 처리가 전부 0ms 로 찍히고, 그러면 p95 가 0.000ms 인데
    // 평균은 0.043ms 인 이상한 결과가 나온다. Go 컨슈머는 나노초까지 재므로
    // 같은 해상도로 맞춰야 비교가 성립한다.
    const startedAt = performance.now();
    try {
      await this.process(topic, partition, message.offset, message.value);
      const finishedAt = performance.now();
      const now = Date.now();
      if (this.stats.processed === 0) {
        this.stats.firstProcessedAt = now;
      }
      this.stats.processed++;
      this.stats.partitionCounts[partition] =
        (this.stats.partitionCounts[partition] ?? 0) + 1;
      this.stats.lastProcessedAt = now;
      this.stats.latenciesMs.push(finishedAt - startedAt);
    } catch (error) {
      this.stats.failed++;
      // 실패를 조용히 넘기면 처리된 것처럼 보인다. 어느 건이 왜 실패했는지 남긴다.
      const reason = error instanceof Error ? error.message : String(error);
      // eslint-disable-next-line no-console
      console.error(
        `[${this.consumerId}] 처리 실패 topic=${topic} partition=${partition} offset=${message.offset}: ${reason}`,
      );
    }

    // 지정한 건수에 도달하면 커밋하지 않고 이 컨슈머만 빠진다.
    // 방금 처리한 건은 커밋되지 않았으므로 재분배 후 다른 컨슈머가 다시 읽는다(중복).
    if (
      this.config.crashAfter > 0 &&
      this.stats.processed >= this.config.crashAfter
    ) {
      this.crashed = true;
      await this.leaveWithoutCommit();
      this.hooks.onCrashPoint?.(this.stats);
      return;
    }

    if (this.config.commitMode === 'after-process') {
      // 처리와 커밋 사이 간격을 넓혀 그 창에서 죽이기 쉽게 만든다.
      if (this.config.commitDelayMs > 0)
        await this.sleep(this.config.commitDelayMs);
      await this.commit(topic, partition, message.offset);
    }
  }

  private async process(
    topic: string,
    partition: number,
    offset: string,
    value: Buffer | null,
  ): Promise<void> {
    if (!value) {
      throw new Error('메시지 본문이 비어 있습니다.');
    }

    const event = this.parseEvent(value, topic, offset);

    if (this.config.processingDelayMs > 0) {
      await this.sleep(this.config.processingDelayMs);
    }

    // 중복을 막지 않는다. 같은 이벤트가 두 번 오면 두 행이 쌓여야 그게 보인다.
    await this.store.recordEvent({
      orderId: event.orderId,
      eventType: event.eventType,
      topic,
      partition,
      offset,
      consumerId: this.consumerId,
      consumedAt: Date.now(),
    });
  }

  private parseEvent(
    value: Buffer,
    topic: string,
    offset: string,
  ): OrderEventPayload {
    let parsed: unknown;
    try {
      parsed = JSON.parse(value.toString());
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(
        `본문을 읽지 못했습니다 topic=${topic} offset=${offset}: ${reason}`,
      );
    }

    const event = parsed as Partial<OrderEventPayload>;
    // 빈 값으로 메우면 어느 주문인지 모르는 기록이 쌓인다. 깨진 건 깨진 대로 알린다.
    if (!event.orderId || !event.eventType) {
      throw new Error(
        `필수 항목이 없습니다 topic=${topic} offset=${offset}: orderId·eventType 확인 필요`,
      );
    }
    if (!Object.values(OrderEventType).includes(event.eventType)) {
      throw new Error(`알 수 없는 이벤트 종류입니다: ${event.eventType}`);
    }

    return event as OrderEventPayload;
  }

  /**
   * 오프셋을 커밋한다.
   *
   * 카프카는 "다음에 읽을 위치" 를 기록하므로 방금 처리한 오프셋 + 1 을 넘긴다.
   * 그냥 offset 을 넘기면 재시작 때 같은 건을 다시 읽는다.
   */
  private async commit(
    topic: string,
    partition: number,
    offset: string,
  ): Promise<void> {
    await this.consumer.commitOffsets([
      { topic, partition, offset: (Number(offset) + 1).toString() },
    ]);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
