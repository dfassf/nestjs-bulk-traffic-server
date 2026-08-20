import { EngineRouterService } from './engine-router.service';
import { GoEngineClient } from './go-engine.client';
import { KafkaProducerBackend } from './kafka-producer.backend';

describe('EngineRouterService', () => {
  let goEngineClient: GoEngineClient;
  let kafkaProducerBackend: KafkaProducerBackend;

  beforeEach(() => {
    goEngineClient = {
      execute: jest.fn(),
    } as unknown as GoEngineClient;
    kafkaProducerBackend = {
      execute: jest.fn(),
    } as unknown as KafkaProducerBackend;
  });

  it('should default to node engine', () => {
    delete process.env.WORKER_ENGINE;
    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);
    expect(service.getEngine()).toBe('node');
  });

  it('should use go engine when WORKER_ENGINE=go', () => {
    process.env.WORKER_ENGINE = 'go';
    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);
    expect(service.getEngine()).toBe('go');
  });

  it('should use both engine when WORKER_ENGINE=both', () => {
    process.env.WORKER_ENGINE = 'both';
    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);
    expect(service.getEngine()).toBe('both');
  });

  it('should use kafka engine when WORKER_ENGINE=kafka', () => {
    process.env.WORKER_ENGINE = 'kafka';
    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);
    expect(service.getEngine()).toBe('kafka');
  });

  it('should fall back to node when WORKER_ENGINE is unknown', () => {
    process.env.WORKER_ENGINE = 'redis';
    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);
    expect(service.getEngine()).toBe('node');
  });

  it('should dispatch to go engine', async () => {
    process.env.WORKER_ENGINE = 'go';
    const mockResult = {
      taskId: '1',
      success: true,
      result: { data: 'test' },
      error: '',
      durationMs: 10,
      engine: 'go',
    };
    (goEngineClient.execute as jest.Mock).mockResolvedValue(mockResult);

    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);

    const task = {
      id: 1,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 5,
    } as any;

    const result = await service.dispatchToGo(task);
    expect(result.success).toBe(true);
    expect(result.engine).toBe('go');
  });

  it('should dispatch to kafka backend', async () => {
    process.env.WORKER_ENGINE = 'kafka';
    const mockResult = {
      mode: 'async' as const,
      taskId: '1',
      dispatch: { topic: 'tasks.high', partition: 0, offset: '42' },
      backend: 'kafka',
    };
    (kafkaProducerBackend.execute as jest.Mock).mockResolvedValue(mockResult);

    const service = new EngineRouterService(goEngineClient, kafkaProducerBackend);

    const task = {
      id: 1,
      execute: async () => null,
      resolve: () => {},
      reject: () => {},
      timestamp: Date.now(),
      priority: 5,
    } as any;

    const result = await service.dispatchToKafka(task);
    expect(result.backend).toBe('kafka');
    if (result.mode === 'async') {
      expect(result.dispatch.topic).toBe('tasks.high');
      expect(result.dispatch.offset).toBe('42');
    }
  });

  afterEach(() => {
    delete process.env.WORKER_ENGINE;
  });
});
