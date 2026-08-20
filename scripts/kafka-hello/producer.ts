import { Kafka, logLevel } from 'kafkajs';

/**
 * kafkajs 프로듀서 Hello World
 *
 * 실행:
 *   npx ts-node scripts/kafka-hello/producer.ts
 *
 * 동작:
 *   tasks.high 토픽에 서로 다른 키로 메시지 5개를 발행한다.
 *   같은 키는 같은 파티션으로 라우팅되는지 확인한다.
 */
async function main() {
  const kafka = new Kafka({
    clientId: 'kafka-hello-producer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.WARN,
  });

  const producer = kafka.producer();
  await producer.connect();
  console.log('[producer] connected');

  const messages = [
    { key: 'user_123', value: { text: 'A번 메시지', at: new Date().toISOString() } },
    { key: 'user_123', value: { text: 'B번 메시지 (같은 키)', at: new Date().toISOString() } },
    { key: 'user_456', value: { text: 'C번 메시지 (다른 키)', at: new Date().toISOString() } },
    { key: 'user_789', value: { text: 'D번 메시지 (또 다른 키)', at: new Date().toISOString() } },
    { key: 'user_123', value: { text: 'E번 메시지 (다시 첫 키)', at: new Date().toISOString() } },
  ];

  const result = await producer.send({
    topic: 'tasks.high',
    messages: messages.map((m) => ({
      key: m.key,
      value: JSON.stringify(m.value),
    })),
  });

  console.log('[producer] sent 5 messages');
  console.log('[producer] result:', JSON.stringify(result, null, 2));

  await producer.disconnect();
  console.log('[producer] disconnected');
}

main().catch((err) => {
  console.error('[producer] error:', err);
  process.exit(1);
});
