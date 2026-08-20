import { Test } from '@nestjs/testing';
import { QueueModule } from '../src/queue/queue.module';
import { EngineRouterService } from '../src/queue/engine-router.service';
import { KafkaProducerBackend } from '../src/queue/kafka-producer.backend';
import { GoEngineClient } from '../src/queue/go-engine.client';
import { DEFAULT_BROKERS, isKafkaAvailable } from './helpers/kafka-availability';

/**
 * QueueModule 부팅 시 KafkaProducerBackend 가 DI 로 정상 주입되고,
 * EngineRouterService.dispatchToKafka() 가 어댑터 인스턴스와 연결되는지 검증한다.
 *
 * 실 브로커 미기동 환경에선 자동 스킵.
 */
describe('QueueModule DI 통합 (Kafka 백엔드)', () => {
  const brokers = DEFAULT_BROKERS;
  let available = false;

  beforeAll(async () => {
    available = await isKafkaAvailable(brokers);
    if (!available) {
      // eslint-disable-next-line no-console
      console.warn(`[e2e] Kafka 브로커(${brokers.join(',')}) 미연결로 스위트 스킵`);
    }
  });

  it('WORKER_ENGINE=kafka 로 부팅 시 dispatchToKafka 가 실 발행을 수행한다', async () => {
    if (!available) {
      // eslint-disable-next-line no-console
      console.warn('[e2e] Kafka 미연결 상태 - 이 테스트는 스킵됩니다');
      return;
    }
    process.env.WORKER_ENGINE = 'kafka';
    process.env.KAFKA_BROKERS = brokers.join(',');
    process.env.KAFKA_CLIENT_ID = 'e2e-module-integration';
    process.env.DISABLE_WORKERS = 'true';
    process.env.QUEUE_PERSISTENCE = 'none';
    process.env.BENCH_DB_DRIVER = 'sqlite';
    process.env.BENCH_DB_PATH = '.bench.e2e.sqlite';

    const moduleRef = await Test.createTestingModule({
      imports: [QueueModule],
    })
      // Go 엔진은 e2e 에서 안 쓰므로 스텁으로 대체
      .overrideProvider(GoEngineClient)
      .useValue({
        isConnected: () => false,
        onModuleInit: jest.fn(),
        onModuleDestroy: jest.fn(),
        execute: jest.fn(),
        getStats: jest.fn(),
        healthCheck: jest.fn(),
      })
      .compile();

    const app = moduleRef.createNestApplication();
    await app.init();

    const router = app.get(EngineRouterService);
    const backend = app.get(KafkaProducerBackend);

    expect(router.getEngine()).toBe('kafka');
    expect(backend.isConnected()).toBe(true);

    const result = await router.dispatchToKafka({
      id: Date.now(),
      requestId: `di-e2e-${Date.now()}`,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 3,
      category: 'di-e2e',
      params: { via: 'router' },
    } as any);

    expect(result.backend).toBe('kafka');
    if (result.mode !== 'async') throw new Error('async 모드여야 함');
    expect(result.dispatch.topic).toBe('tasks.normal');
    expect(Number(result.dispatch.offset)).toBeGreaterThanOrEqual(0);

    await app.close();

    delete process.env.WORKER_ENGINE;
    delete process.env.KAFKA_BROKERS;
    delete process.env.KAFKA_CLIENT_ID;
    delete process.env.DISABLE_WORKERS;
    delete process.env.QUEUE_PERSISTENCE;
    delete process.env.BENCH_DB_DRIVER;
    delete process.env.BENCH_DB_PATH;
  });
});
