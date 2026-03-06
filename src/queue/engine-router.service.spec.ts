import { EngineRouterService } from './engine-router.service';
import { GoEngineClient } from './go-engine.client';

describe('EngineRouterService', () => {
  let goEngineClient: GoEngineClient;

  beforeEach(() => {
    goEngineClient = {
      execute: jest.fn(),
    } as unknown as GoEngineClient;
  });

  it('should default to node engine', () => {
    delete process.env.WORKER_ENGINE;
    const service = new EngineRouterService(goEngineClient);
    expect(service.getEngine()).toBe('node');
  });

  it('should use go engine when WORKER_ENGINE=go', () => {
    process.env.WORKER_ENGINE = 'go';
    const service = new EngineRouterService(goEngineClient);
    expect(service.getEngine()).toBe('go');
  });

  it('should use both engine when WORKER_ENGINE=both', () => {
    process.env.WORKER_ENGINE = 'both';
    const service = new EngineRouterService(goEngineClient);
    expect(service.getEngine()).toBe('both');
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

    const service = new EngineRouterService(goEngineClient);

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

  afterEach(() => {
    delete process.env.WORKER_ENGINE;
  });
});
