import { Admin, Consumer, Kafka, logLevel } from 'kafkajs';
import { KafkaProducerBackend } from '../src/queue/kafka-producer.backend';
import { QueueTask, WorkloadType } from '../src/queue/interfaces/queue-task.interface';
import { DEFAULT_BROKERS, isKafkaAvailable } from './helpers/kafka-availability';

/**
 * Kafka 프로듀서 백엔드 실 브로커 e2e.
 *
 * localhost:9092 브로커가 살아있어야 실행. 미가동 시 자동 스킵.
 * docker compose up -d kafka 로 미리 띄우고 실행할 것.
 */
describe('KafkaProducerBackend (실 브로커 e2e)', () => {
  const brokers = DEFAULT_BROKERS;
  const e2eTopics = ['tasks.high', 'tasks.normal', 'tasks.low'] as const;

  let backend: KafkaProducerBackend;
  let admin: Admin;
  let kafka: Kafka;
  let available = false;

  beforeAll(async () => {
    available = await isKafkaAvailable(brokers);
    if (!available) {
      // eslint-disable-next-line no-console
      console.warn(`[e2e] Kafka 브로커(${brokers.join(',')}) 미연결로 스위트 스킵`);
      return;
    }

    process.env.WORKER_ENGINE = 'kafka';
    process.env.KAFKA_BROKERS = brokers.join(',');
    process.env.KAFKA_CLIENT_ID = 'e2e-producer';

    backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    kafka = new Kafka({
      clientId: 'e2e-verifier',
      brokers,
      logLevel: logLevel.NOTHING,
    });
    admin = kafka.admin();
    await admin.connect();

    // 3개 토픽 준비 (이미 있으면 카프카가 무시)
    await admin.createTopics({
      waitForLeaders: true,
      topics: e2eTopics.map((topic) => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1,
      })),
    });
  });

  afterAll(async () => {
    if (backend) await backend.onModuleDestroy();
    if (admin) await admin.disconnect();
    delete process.env.WORKER_ENGINE;
    delete process.env.KAFKA_BROKERS;
    delete process.env.KAFKA_CLIENT_ID;
  });

  function skipIfUnavailable(): boolean {
    if (!available) {
      // eslint-disable-next-line no-console
      console.warn('[e2e] Kafka 미연결 상태 - 이 테스트는 스킵됩니다');
      return true;
    }
    return false;
  }

  function buildTask(overrides: Partial<QueueTask> = {}): QueueTask {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    return {
      id,
      requestId: `e2e-req-${id}`,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 0,
      workloadType: WorkloadType.CPU,
      category: 'e2e',
      params: { source: 'e2e' },
      ...overrides,
    };
  }

  async function readOne(topic: string, partition: number, offset: string): Promise<{ key: string; value: string } | null> {
    const consumer: Consumer = kafka.consumer({ groupId: `e2e-verify-${topic}-${offset}-${Date.now()}` });
    await consumer.connect();
    try {
      // fromBeginning 대신 seek로 정확한 오프셋 소비 (offset+1 까지만 대기)
      await consumer.subscribe({ topic, fromBeginning: false });
      const result = await new Promise<{ key: string; value: string } | null>((resolve, reject) => {
        const timer = setTimeout(() => resolve(null), 8000);
        consumer
          .run({
            eachMessage: async ({ topic: t, partition: p, message }) => {
              if (t === topic && p === partition && message.offset === offset) {
                clearTimeout(timer);
                resolve({
                  key: message.key?.toString() ?? '',
                  value: message.value?.toString() ?? '',
                });
              }
            },
          })
          .catch(reject);
        // subscribe 후 offset 지정 (run 다음에 seek 호출)
        setTimeout(() => {
          try {
            consumer.seek({ topic, partition, offset });
          } catch (err) {
            clearTimeout(timer);
            reject(err);
          }
        }, 200);
      });
      return result;
    } finally {
      await consumer.disconnect();
    }
  }

  it('priority 7 발행 시 실제 tasks.high 오프셋에 저장된다', async () => {
    if (skipIfUnavailable()) return;
    const task = buildTask({ priority: 7 });
    const result = await backend.execute(task);

    expect(result.backend).toBe('kafka');
    if (result.mode !== 'async') throw new Error('async 모드여야 함');
    expect(result.dispatch.topic).toBe('tasks.high');
    expect(Number(result.dispatch.offset)).toBeGreaterThanOrEqual(0);

    const read = await readOne(result.dispatch.topic, result.dispatch.partition, result.dispatch.offset);
    expect(read).not.toBeNull();
    expect(read!.key).toBe(String(task.requestId));

    const payload = JSON.parse(read!.value);
    expect(payload.taskId).toBe(task.id);
    expect(payload.priority).toBe(7);
    expect(payload.params).toEqual({ source: 'e2e' });
  });

  it('priority 0 → tasks.normal, priority -1 → tasks.low 로 저장된다', async () => {
    if (skipIfUnavailable()) return;
    const normalTask = buildTask({ priority: 0 });
    const lowTask = buildTask({ priority: -1 });

    const normalResult = await backend.execute(normalTask);
    const lowResult = await backend.execute(lowTask);

    if (normalResult.mode !== 'async' || lowResult.mode !== 'async') {
      throw new Error('async 모드여야 함');
    }
    expect(normalResult.dispatch.topic).toBe('tasks.normal');
    expect(lowResult.dispatch.topic).toBe('tasks.low');
  });

  it('같은 requestId 로 두 번 발행 시 같은 파티션에 저장된다 (key 라우팅 안정성)', async () => {
    if (skipIfUnavailable()) return;
    const requestId = `stable-key-${Date.now()}`;
    const t1 = buildTask({ priority: 2, requestId });
    const t2 = buildTask({ priority: 2, requestId });

    const r1 = await backend.execute(t1);
    const r2 = await backend.execute(t2);

    if (r1.mode !== 'async' || r2.mode !== 'async') throw new Error('async 모드여야 함');
    expect(r1.dispatch.topic).toBe(r2.dispatch.topic);
    expect(r1.dispatch.partition).toBe(r2.dispatch.partition);
  });

  it('healthCheck 는 브로커 정상 상태에서 true 를 반환한다', async () => {
    if (skipIfUnavailable()) return;
    const ok = await backend.healthCheck!();
    expect(ok).toBe(true);
  });
});
