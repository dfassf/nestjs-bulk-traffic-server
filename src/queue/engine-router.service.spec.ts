import { EngineRouterService } from './engine-router.service';
import { GoEngineClient } from './go-engine.client';
import { GoEngineBackend } from './go-engine.backend';
import { KafkaProducerBackend } from './kafka-producer.backend';

describe('EngineRouterService', () => {
  let goEngineClient: GoEngineClient;
  let goEngineBackend: GoEngineBackend;
  let kafkaProducerBackend: KafkaProducerBackend;

  const buildRouter = () =>
    new EngineRouterService(
      goEngineClient,
      goEngineBackend,
      kafkaProducerBackend,
    );

  beforeEach(() => {
    goEngineClient = {
      execute: jest.fn(),
    } as unknown as GoEngineClient;
    goEngineBackend = {
      name: 'go',
      execute: jest.fn(),
    } as unknown as GoEngineBackend;
    kafkaProducerBackend = {
      name: 'kafka',
      execute: jest.fn(),
    } as unknown as KafkaProducerBackend;
  });

  afterEach(() => {
    delete process.env.WORKER_ENGINE;
  });

  it('WORKER_ENGINE 미설정이면 node 엔진이다', () => {
    delete process.env.WORKER_ENGINE;
    expect(buildRouter().getEngine()).toBe('node');
  });

  it.each(['go', 'both', 'kafka'])(
    'WORKER_ENGINE=%s 를 그대로 쓴다',
    (engine) => {
      process.env.WORKER_ENGINE = engine;
      expect(buildRouter().getEngine()).toBe(engine);
    },
  );

  it('대소문자·공백이 섞여도 같은 엔진으로 해석한다', () => {
    process.env.WORKER_ENGINE = ' Kafka ';
    expect(buildRouter().getEngine()).toBe('kafka');
  });

  it('알 수 없는 값이면 node 로 떨어진다 (부팅 차단은 validateEnv 담당)', () => {
    process.env.WORKER_ENGINE = 'redis';
    expect(buildRouter().getEngine()).toBe('node');
  });

  describe('getBackend', () => {
    it('kafka 모드면 Kafka 백엔드를 돌려준다', () => {
      process.env.WORKER_ENGINE = 'kafka';
      expect(buildRouter().getBackend()).toBe(kafkaProducerBackend);
    });

    it('go 모드면 Go 백엔드를 돌려준다', () => {
      process.env.WORKER_ENGINE = 'go';
      expect(buildRouter().getBackend()).toBe(goEngineBackend);
    });

    it.each(['node', 'both'])(
      '%s 모드는 워커풀·메인스레드가 맡으므로 null 이다',
      (engine) => {
        process.env.WORKER_ENGINE = engine;
        expect(buildRouter().getBackend()).toBeNull();
      },
    );
  });

  it('dispatchToGo 는 엔진 모드와 무관하게 Go 엔진으로 보낸다', async () => {
    process.env.WORKER_ENGINE = 'go';
    (goEngineClient.execute as jest.Mock).mockResolvedValue({
      taskId: '1',
      success: true,
      result: { data: 'test' },
      error: '',
      durationMs: 10,
      engine: 'go',
    });

    const task = {
      id: 1,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 5,
    } as any;

    const result = await buildRouter().dispatchToGo(task);
    expect(result.success).toBe(true);
    expect(result.engine).toBe('go');
  });

  it('dispatchToKafka 는 Kafka 백엔드로 위임한다', async () => {
    process.env.WORKER_ENGINE = 'kafka';
    (kafkaProducerBackend.execute as jest.Mock).mockResolvedValue({
      mode: 'async' as const,
      taskId: '1',
      dispatch: { topic: 'tasks.high', partition: 0, offset: '42' },
      backend: 'kafka',
    });

    const task = {
      id: 1,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 5,
    } as any;

    const result = await buildRouter().dispatchToKafka(task);
    expect(result.backend).toBe('kafka');
    if (result.mode !== 'async') throw new Error('async 모드여야 함');
    expect(result.dispatch.topic).toBe('tasks.high');
    expect(result.dispatch.offset).toBe('42');
  });
});
