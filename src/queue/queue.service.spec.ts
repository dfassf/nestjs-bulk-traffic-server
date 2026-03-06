import { Test, TestingModule } from '@nestjs/testing';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { WorkerPoolService } from './worker-pool.service';
import { QueueOptionsParser } from './queue-options.parser';
import { QueueStatsService } from './queue-stats.service';
import { QueueSnapshotManager } from './queue-snapshot.manager';
import { QueueStateHolder } from './queue-state.holder';
import { QueueProcessorService } from './queue-processor.service';
import { WorkerHealthService } from './worker-health.service';
import { WorkerTaskRouterService } from './worker-task-router.service';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

const ALL_PROVIDERS = [
  QueueService,
  QueueStateHolder,
  QueueProcessorService,
  MemoryService,
  BatchService,
  WorkerPoolService,
  QueueOptionsParser,
  QueueStatsService,
  QueueSnapshotManager,
  WorkerHealthService,
  WorkerTaskRouterService,
];

describe('QueueService', () => {
  const originalAllowCustomWorkload = process.env.ALLOW_CUSTOM_WORKLOAD;

  let service: QueueService;
  let processor: QueueProcessorService;
  let memoryService: MemoryService;
  let workerPoolService: WorkerPoolService;
  let statsService: QueueStatsService;

  const createTask = (id: number): QueueTask => ({
    id,
    execute: jest.fn().mockResolvedValue('ok'),
    resolve: jest.fn(),
    reject: jest.fn(),
    timestamp: Date.now(),
    priority: 0,
    size: 1,
    category: 'test',
  });

  beforeEach(async () => {
    process.env.ALLOW_CUSTOM_WORKLOAD = 'false';

    const module: TestingModule = await Test.createTestingModule({
      providers: ALL_PROVIDERS,
    }).compile();

    service = module.get<QueueService>(QueueService);
    processor = module.get<QueueProcessorService>(QueueProcessorService);
    memoryService = module.get<MemoryService>(MemoryService);
    workerPoolService = module.get<WorkerPoolService>(WorkerPoolService);
    statsService = module.get<QueueStatsService>(QueueStatsService);
  });

  afterEach(async () => {
    await service.onModuleDestroy();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  afterAll(() => {
    process.env.ALLOW_CUSTOM_WORKLOAD = originalAllowCustomWorkload;
  });

  describe('enqueue', () => {
    it('작업을 큐에 추가하고 실행해야 한다', async () => {
      const promise = service.enqueue(() => Promise.resolve('success'));
      processor.processQueue();

      await expect(promise).resolves.toBe('success');
    });

    it('높은 우선순위 작업이 먼저 처리되어야 한다', async () => {
      const order: number[] = [];

      const lowPromise = service.enqueue(
        () => {
          order.push(1);
          return Promise.resolve(1);
        },
        { priority: -1 },
      );

      const highPromise = service.enqueue(
        () => {
          order.push(2);
          return Promise.resolve(2);
        },
        { priority: 10 },
      );

      processor.processQueue();
      await Promise.all([lowPromise, highPromise]);

      expect(order[0]).toBe(2);
    });

    it('메모리 압박 시 저우선순위 작업을 거부해야 한다', async () => {
      Object.defineProperty(memoryService, 'memoryPressure', {
        configurable: true,
        get: () => true,
      });

      await expect(
        service.enqueue(() => Promise.resolve(), { priority: -1 }),
      ).rejects.toThrow('서버 과부하로 요청이 거부되었습니다.');
    });

    it('큐 오버플로우 계산에 배치 큐 작업도 포함해야 한다', async () => {
      const state = (service as any).state as QueueStateHolder;
      state.normalPriorityQueue.length = 2999;
      state.batchQueues.set('batch-overflow', {
        tasks: [createTask(1)],
        category: 'batch-overflow',
        totalSize: 1,
        createdAt: Date.now(),
      });

      await expect(service.enqueue(() => Promise.resolve())).rejects.toThrow(
        '큐가 가득 찼습니다.',
      );
    });

    it('메모리 압박 중에는 기존 저우선순위 대기를 소비하지 않아야 한다', async () => {
      const lowPromise = service.enqueue(() => Promise.resolve('low'), {
        priority: -1,
      });
      const normalPromise = service.enqueue(() => Promise.resolve('normal'), {
        priority: 0,
      });

      Object.defineProperty(memoryService, 'memoryPressure', {
        configurable: true,
        get: () => true,
      });

      processor.processQueue();
      await expect(normalPromise).resolves.toBe('normal');

      const pausedStats = service.getQueueStats();
      expect(pausedStats.lowPriorityQueueLength).toBe(1);

      Object.defineProperty(memoryService, 'memoryPressure', {
        configurable: true,
        get: () => false,
      });

      processor.processQueue();
      await expect(lowPromise).resolves.toBe('low');
    });

    it('실행 시간이 timeout을 초과하면 실행 타임아웃 에러를 반환해야 한다', async () => {
      jest.useFakeTimers();

      const promise = service.enqueue(
        () =>
          new Promise<string>((resolve) => {
            setTimeout(() => resolve('late'), 1000);
          }),
        { timeout: 10 },
      );
      const assertion = expect(promise).rejects.toThrow(
        '작업 실행 시간 초과 (10ms)',
      );

      processor.processQueue();
      await jest.advanceTimersByTimeAsync(20);
      await assertion;
    });

    it('알 수 없는 workloadType은 일반 큐로 fallback 되어야 한다', async () => {
      Object.defineProperty(workerPoolService, 'isEnabled', {
        configurable: true,
        get: () => true,
      });
      const addTaskSpy = jest.spyOn(workerPoolService, 'addTask');

      const promise = service.enqueue(() => Promise.resolve('fallback-ok'), {
        workloadType: 'gpu',
      });

      processor.processQueue();
      await expect(promise).resolves.toBe('fallback-ok');
      expect(addTaskSpy).not.toHaveBeenCalled();
      expect(service.getQueueStats().workloadGeneralQueueFallbackCount).toBe(1);
    });

    it('명시한 workloadType이 cpu면 워커 대상 작업으로 분류되어야 한다', () => {
      Object.defineProperty(workerPoolService, 'isEnabled', {
        configurable: true,
        get: () => true,
      });

      const eligible = (processor as any).isWorkerEligibleTask({
        ...createTask(99),
        workloadType: WorkloadType.CPU,
      });

      expect(eligible).toBe(true);
    });

    it('입력 검증: size는 양의 정수여야 한다', async () => {
      await expect(
        service.enqueue(() => Promise.resolve('invalid'), {
          size: 0,
        }),
      ).rejects.toThrow('size 값은 0보다 큰 정수여야 합니다.');
    });

    it('custom workload 비활성화 시 functionCode 입력은 거부되어야 한다', async () => {
      await expect(
        service.enqueue(() => Promise.resolve('blocked'), {
          workloadType: WorkloadType.CUSTOM,
          functionCode: 'return 1;',
        }),
      ).rejects.toThrow('custom workload 기능이 비활성화되어 있습니다.');
    });

    it('custom workload 활성화 시 functionCode 입력을 허용해야 한다', async () => {
      process.env.ALLOW_CUSTOM_WORKLOAD = 'true';
      const module: TestingModule = await Test.createTestingModule({
        providers: ALL_PROVIDERS,
      }).compile();

      const enabledService = module.get<QueueService>(QueueService);
      const enabledProcessor = module.get<QueueProcessorService>(QueueProcessorService);
      const promise = enabledService.enqueue(() => Promise.resolve('ok'), {
        workloadType: WorkloadType.CUSTOM,
        functionCode: 'return 1;',
      });

      enabledProcessor.processQueue();
      await expect(promise).resolves.toBe('ok');
      await enabledService.onModuleDestroy();

      process.env.ALLOW_CUSTOM_WORKLOAD = 'false';
    });

    it('배치 작업이 대기 타임아웃되면 배치 큐에서도 제거되어야 한다', async () => {
      jest.useFakeTimers();

      const promise = service.enqueue(() => Promise.resolve('batch'), {
        batch: true,
        category: 'batch-timeout',
        priority: 0,
        size: 1,
      });
      const assertion = expect(promise).rejects.toThrow('큐 대기 시간 초과');

      expect(service.getQueueStats().batchTaskCount).toBe(1);

      await jest.advanceTimersByTimeAsync(15001);
      await assertion;

      const stats = service.getQueueStats();
      expect(stats.batchTaskCount).toBe(0);
      expect(stats.batchQueueCount).toBe(0);
    });
  });

  describe('stats', () => {
    it('누적 통계와 최근 통계를 분리해 유지해야 한다', async () => {
      const promise = service.enqueue(() => Promise.resolve('success'));
      processor.processQueue();
      await promise;

      const beforeLog = service.getQueueStats();
      expect(beforeLog.totalProcessed).toBe(1);
      expect(beforeLog.recentProcessed).toBe(1);

      statsService.logStats(0, 0, 0);

      const afterLog = service.getQueueStats();
      expect(afterLog.totalProcessed).toBe(1);
      expect(afterLog.recentProcessed).toBe(0);
    });

    it('onModuleDestroy에서 interval을 정리해야 한다', async () => {
      jest.useFakeTimers();
      const clearIntervalSpy = jest.spyOn(global, 'clearInterval');

      await service.onModuleInit();
      await service.onModuleDestroy();

      expect(clearIntervalSpy).toHaveBeenCalledTimes(4);
    });
  });

  describe('getQueueStats', () => {
    it('큐 통계를 반환해야 한다', () => {
      const stats = service.getQueueStats();

      expect(stats).toHaveProperty('highPriorityQueueLength');
      expect(stats).toHaveProperty('normalPriorityQueueLength');
      expect(stats).toHaveProperty('lowPriorityQueueLength');
      expect(stats).toHaveProperty('batchTaskCount');
      expect(stats).toHaveProperty('activeRequests');
      expect(stats).toHaveProperty('memoryPressure');
      expect(stats).toHaveProperty('persistence');
      expect(stats.persistence.enabled).toBe(false);
      expect(stats.highPriorityQueueLength).toBe(0);
      expect(stats.normalPriorityQueueLength).toBe(0);
      expect(stats.lowPriorityQueueLength).toBe(0);
    });
  });
});
