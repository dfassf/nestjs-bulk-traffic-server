import { EngineRouterService } from './engine-router.service';
import { WorkerPoolService } from './worker-pool.service';
import { GoEngineClient } from './go-engine.client';
import { BenchmarkService } from './benchmark.service';

describe('EngineRouterService', () => {
  let workerPoolService: WorkerPoolService;
  let goEngineClient: GoEngineClient;
  let benchmarkService: BenchmarkService;

  beforeEach(() => {
    workerPoolService = {} as WorkerPoolService;
    goEngineClient = {
      execute: jest.fn(),
    } as unknown as GoEngineClient;
    benchmarkService = new BenchmarkService();
  });

  it('should default to node engine', () => {
    delete process.env.WORKER_ENGINE;
    const service = new EngineRouterService(
      workerPoolService,
      goEngineClient,
      benchmarkService,
    );
    expect(service.getEngine()).toBe('node');
    expect(service.isGoEnabled()).toBe(false);
  });

  it('should use go engine when WORKER_ENGINE=go', () => {
    process.env.WORKER_ENGINE = 'go';
    const service = new EngineRouterService(
      workerPoolService,
      goEngineClient,
      benchmarkService,
    );
    expect(service.getEngine()).toBe('go');
    expect(service.isGoEnabled()).toBe(true);
  });

  it('should use both engine when WORKER_ENGINE=both', () => {
    process.env.WORKER_ENGINE = 'both';
    const service = new EngineRouterService(
      workerPoolService,
      goEngineClient,
      benchmarkService,
    );
    expect(service.getEngine()).toBe('both');
    expect(service.isGoEnabled()).toBe(true);
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

    const service = new EngineRouterService(
      workerPoolService,
      goEngineClient,
      benchmarkService,
    );

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
