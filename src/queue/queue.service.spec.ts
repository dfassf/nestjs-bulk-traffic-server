import { Test, TestingModule } from '@nestjs/testing';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';

describe('QueueService', () => {
  let service: QueueService;
  let memoryService: MemoryService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [QueueService, MemoryService, BatchService],
    }).compile();

    service = module.get<QueueService>(QueueService);
    memoryService = module.get<MemoryService>(MemoryService);

    // onModuleInit의 setInterval을 방지
    jest.spyOn(service, 'onModuleInit').mockImplementation(async () => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('enqueue', () => {
    it('작업을 큐에 추가하고 실행해야 한다', async () => {
      const promise = service.enqueue(() => Promise.resolve('success'));

      // onModuleInit이 mock되어 setInterval이 없으므로 직접 processQueue 호출
      (service as any).processQueue();

      const result = await promise;
      expect(result).toBe('success');
    });

    it('높은 우선순위 작업이 먼저 처리되어야 한다', async () => {
      const order: number[] = [];

      const p1 = service.enqueue(
        () => {
          order.push(1);
          return Promise.resolve(1);
        },
        { priority: -1 },
      );
      const p2 = service.enqueue(
        () => {
          order.push(2);
          return Promise.resolve(2);
        },
        { priority: 10 },
      );

      // 수동으로 큐 처리 - high → normal → low 순서로 실행됨
      (service as any).processQueue();

      await Promise.all([p1, p2]);
      // 높은 우선순위(10)가 낮은 우선순위(-1)보다 먼저 처리
      expect(order[0]).toBe(2);
    });

    it('메모리 압박 시 저우선순위 작업을 거부해야 한다', async () => {
      Object.defineProperty(memoryService, 'memoryPressure', {
        get: () => true,
      });

      await expect(
        service.enqueue(() => Promise.resolve(), { priority: -1 }),
      ).rejects.toThrow('서버 과부하로 요청이 거부되었습니다.');
    });

    it('큐 오버플로우 시 작업을 거부해야 한다', async () => {
      // 큐를 가득 채우기 위해 내부 상태 접근
      const queueService = service as any;
      queueService.normalPriorityQueue = Array(3000).fill({});

      await expect(
        service.enqueue(() => Promise.resolve()),
      ).rejects.toThrow('큐가 가득 찼습니다.');
    });
  });

  describe('getQueueStats', () => {
    it('큐 통계를 반환해야 한다', () => {
      const stats = service.getQueueStats();

      expect(stats).toHaveProperty('highPriorityQueueLength');
      expect(stats).toHaveProperty('normalPriorityQueueLength');
      expect(stats).toHaveProperty('lowPriorityQueueLength');
      expect(stats).toHaveProperty('activeRequests');
      expect(stats).toHaveProperty('memoryPressure');
      expect(stats.highPriorityQueueLength).toBe(0);
      expect(stats.normalPriorityQueueLength).toBe(0);
      expect(stats.lowPriorityQueueLength).toBe(0);
    });
  });
});
