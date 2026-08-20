import { Kafka, logLevel } from 'kafkajs';

/**
 * kafkajs 컨슈머 Hello World
 *
 * 실행:
 *   npx ts-node scripts/kafka-hello/consumer.ts
 *
 * 동작:
 *   tasks.high 토픽을 구독하고 들어오는 메시지를 로그로 출력한다.
 *   컨슈머 그룹 이름은 kafka-hello-group.
 *   같은 그룹 이름으로 재시작하면 이전 오프셋 이어서 소비.
 *   그룹 이름 바꾸면 처음부터 다시 소비.
 *
 * 환경변수:
 *   FROM_BEGINNING=true 로 실행하면 처음부터 읽음
 *   GROUP_ID=xxx 로 그룹 이름 커스텀 가능
 */
async function main() {
  const groupId = process.env.GROUP_ID ?? 'kafka-hello-group';
  const fromBeginning = process.env.FROM_BEGINNING === 'true';

  const kafka = new Kafka({
    clientId: 'kafka-hello-consumer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.WARN,
  });

  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  console.log(`[consumer] connected. groupId=${groupId} fromBeginning=${fromBeginning}`);

  await consumer.subscribe({ topic: 'tasks.high', fromBeginning });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString() ?? '(no key)';
      const value = message.value?.toString() ?? '(no value)';
      const offset = message.offset;
      const ts = new Date(Number(message.timestamp)).toISOString();

      console.log(
        `[consumer] topic=${topic} partition=${partition} offset=${offset} key=${key} ts=${ts}\n           value=${value}`,
      );
    },
  });

  const shutdown = async (signal: string) => {
    console.log(`\n[consumer] ${signal} received. disconnecting...`);
    await consumer.disconnect();
    console.log('[consumer] disconnected');
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('[consumer] error:', err);
  process.exit(1);
});
