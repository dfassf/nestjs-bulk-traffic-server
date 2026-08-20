import { Kafka, logLevel } from 'kafkajs';

/**
 * 로컬 카프카 브로커가 살아있는지 빠르게 확인한다.
 * CI 등 브로커 미기동 환경에서 e2e 를 자동 스킵하기 위해 사용.
 */
export async function isKafkaAvailable(brokers: string[]): Promise<boolean> {
  const kafka = new Kafka({
    clientId: 'e2e-availability-check',
    brokers,
    logLevel: logLevel.NOTHING,
    retry: { retries: 0 },
    connectionTimeout: 2000,
    requestTimeout: 2000,
  });
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch {
    return false;
  } finally {
    try {
      await admin.disconnect();
    } catch {
      /* noop */
    }
  }
}

export const DEFAULT_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
