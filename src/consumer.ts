import {
  consumerConfigFromEnv,
  describeConfig,
} from './orders/consumer/consumer-config';
import { OrderConsumer } from './orders/consumer/order-consumer';
import { SqliteOrderStore } from './orders/sqlite-order.store';

/**
 * 주문 이벤트 컨슈머 프로세스.
 *
 * 실행:
 *   node dist/consumer.js
 *   CONSUMER_COUNT=3 CONSUMER_DELAY_MS=100 node dist/consumer.js
 *
 * 서버(main.ts)와 별도 프로세스인 이유:
 * 강제 종료 실험 때문이다. disconnect() 는 정상 종료라 카프카에 나간다고 알리고
 * 오프셋도 커밋하고 빠져서 중복이 안 생긴다. 실무에서 중복이 생기는 건
 * 프로세스가 갑자기 죽어 커밋을 놓친 경우이고, 그건 kill -9 로만 재현된다.
 *
 * 실험 스위치는 docs/kafka-lab-plan.md 와 .env.example 참고.
 */
async function bootstrap(): Promise<void> {
  const config = consumerConfigFromEnv();
  const store = new SqliteOrderStore();
  await store.init();

  console.log(`[consumer] 시작 ${describeConfig(config)}`);
  console.log(`[consumer] 구독 토픽: ${config.topics.join(', ')}`);

  const consumers: OrderConsumer[] = [];
  let shuttingDown = false;

  const shutdown = async (reason: string, exitCode = 0): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log(`[consumer] 종료: ${reason}`);
    printStats(consumers);

    await Promise.all(consumers.map((c) => c.stop().catch(() => undefined)));
    await store.destroy().catch(() => undefined);
    process.exit(exitCode);
  };

  /**
   * 지정 건수에 도달한 컨슈머 하나가 커밋 없이 빠졌을 때.
   *
   * 프로세스를 끝내지 않는다. 예전에는 여기서 process.exit 를 불렀는데,
   * 그러면 같은 프로세스의 다른 컨슈머들이 커밋 직전에 함께 죽어서
   * 실제로는 없었을 중복까지 관측됐다. 실험 숫자가 부풀려지는 것이다.
   *
   * 하나만 빠지면 카프카가 남은 컨슈머들에게 파티션을 다시 나눠준다.
   * 그 재분배 과정도 실험에서 볼 거리다.
   */
  const onCrashPoint = (stats: { consumerId: string; processed: number }) => {
    console.log(
      `[consumer] ${stats.consumerId} 가 ${stats.processed}건 처리 후 커밋 없이 빠집니다. ` +
        '(남은 컨슈머가 그 파티션을 넘겨받아 다시 읽으면 중복이 관측됩니다)',
    );
    printStats(consumers);

    // 전부 빠졌으면 더 할 일이 없다. 살아 있는 게 하나라도 있으면 계속 돈다.
    if (consumers.every((c) => !c.isRunning())) {
      console.log('[consumer] 모든 컨슈머가 빠져 프로세스를 종료합니다.');
      void shutdown('crashAfter 도달', 137);
    }
  };

  for (let i = 0; i < config.instances; i++) {
    const consumerId = `${process.pid}-${i}`;
    const consumer = new OrderConsumer(config, store, consumerId, {
      onCrashPoint,
    });
    consumers.push(consumer);
    await consumer.start();
    console.log(`[consumer] ${consumerId} 준비됨`);
  }

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  // 처리 현황을 주기적으로 보여준다. 파티션 분배가 어떻게 됐는지 여기서 보인다.
  const reportIntervalMs = Number(
    process.env.CONSUMER_REPORT_INTERVAL_MS ?? 10000,
  );
  if (reportIntervalMs > 0) {
    setInterval(() => printStats(consumers), reportIntervalMs).unref();
  }
}

function printStats(consumers: OrderConsumer[]): void {
  const total = consumers.reduce((sum, c) => sum + c.getStats().processed, 0);
  const failed = consumers.reduce((sum, c) => sum + c.getStats().failed, 0);
  if (total === 0 && failed === 0) return;

  console.log(`[consumer] 누적 처리 ${total}건 실패 ${failed}건`);
  for (const consumer of consumers) {
    const stats = consumer.getStats();
    const partitions = Object.entries(stats.partitionCounts)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([p, count]) => `p${p}:${count}`)
      .join(' ');
    // 파티션이 하나도 없으면 이 컨슈머가 놀고 있다는 뜻이다(파티션보다 컨슈머가 많을 때).
    // 빠진 컨슈머는 표시해준다. 안 그러면 처리량이 왜 줄었는지 읽을 수 없다.
    const state = consumer.hasCrashed() ? ' [커밋 없이 빠짐]' : '';
    console.log(
      `  ${stats.consumerId} 처리=${stats.processed} ${partitions || '(할당된 파티션 없음)'}${state}`,
    );
  }
}

bootstrap().catch((error) => {
  console.error('[consumer] 시작 실패:', error);
  process.exit(1);
});
