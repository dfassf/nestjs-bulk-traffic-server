import { Test, TestingModule } from '@nestjs/testing';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { QueueTask } from './interfaces/queue-task.interface';

describe('QueueService', () => {
  let service: QueueService;
  let memoryService: MemoryService;

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
    const module: TestingModule = await Test.createTestingModule({
      providers: [QueueService, MemoryService, BatchService],
    }).compile();

    service = module.get<QueueService>(QueueService);
    memoryService = module.get<MemoryService>(MemoryService);
  });

  afterEach(() => {
    service.onModuleDestroy();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  describe('enqueue', () => {
    it('작업을 큐에 추가하고 실행해야 한다', async () => {
      const promise = service.enqueue(() => Promise.resolve('success'));
      (service as any).processQueue();

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

      (service as any).processQueue();
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
      const queueService = service as any;
      queueService.normalPriorityQueue = Array(2999).fill({});
      queueService.batchQueues.set('batch-overflow', {
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

      (service as any).processQueue();
      await expect(normalPromise).resolves.toBe('normal');

      const pausedStats = service.getQueueStats();
      expect(pausedStats.lowPriorityQueueLength).toBe(1);

      Object.defineProperty(memoryService, 'memoryPressure', {
        configurable: true,
        get: () => false,
      });

      (service as any).processQueue();
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

      (service as any).processQueue();
      await jest.advanceTimersByTimeAsync(20);
      await assertion;
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
      (service as any).processQueue();
      await promise;

      const beforeLog = service.getQueueStats();
      expect(beforeLog.totalProcessed).toBe(1);
      expect(beforeLog.recentProcessed).toBe(1);

      (service as any).logStats();

      const afterLog = service.getQueueStats();
      expect(afterLog.totalProcessed).toBe(1);
      expect(afterLog.recentProcessed).toBe(0);
    });

    it('onModuleDestroy에서 interval을 정리해야 한다', async () => {
      jest.useFakeTimers();
      const clearIntervalSpy = jest.spyOn(global, 'clearInterval');

      await service.onModuleInit();
      service.onModuleDestroy();

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
      expect(stats.highPriorityQueueLength).toBe(0);
      expect(stats.normalPriorityQueueLength).toBe(0);
      expect(stats.lowPriorityQueueLength).toBe(0);
    });
  });
});
