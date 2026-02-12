import { Test, TestingModule } from '@nestjs/testing';
import { MemoryService } from './memory.service';
import { QueueTask } from './interfaces/queue-task.interface';

describe('MemoryService', () => {
  let service: MemoryService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [MemoryService],
    }).compile();

    service = module.get<MemoryService>(MemoryService);
  });

  it('초기 상태에서 메모리 압박이 없어야 한다', () => {
    expect(service.memoryPressure).toBe(false);
  });

  describe('forceReduceQueues', () => {
    it('저우선순위 큐의 50%를 제거해야 한다', () => {
      const lowPriorityQueue: QueueTask[] = Array.from(
        { length: 10 },
        (_, i) => ({
          id: i,
          execute: jest.fn(),
          resolve: jest.fn(),
          reject: jest.fn(),
          timestamp: Date.now(),
          priority: -1,
        }),
      );

      const batchQueues = new Map();
      const result = service.forceReduceQueues(
        lowPriorityQueue,
        batchQueues,
        jest.fn(),
      );

      expect(result).toBe(5);
      expect(lowPriorityQueue.length).toBe(5);
    });

    it('빈 큐에서는 0을 반환해야 한다', () => {
      const result = service.forceReduceQueues([], new Map(), jest.fn());
      expect(result).toBe(0);
    });
  });

  describe('cleanupOldTasks', () => {
    it('만료된 작업을 제거해야 한다', () => {
      const oldTimestamp = Date.now() - 30000;
      const queues = {
        high: [
          {
            id: 1,
            execute: jest.fn(),
            resolve: jest.fn(),
            reject: jest.fn(),
            timestamp: oldTimestamp,
            priority: 5,
          },
        ] as QueueTask[],
        normal: [] as QueueTask[],
        low: [] as QueueTask[],
      };

      const result = service.cleanupOldTasks(queues, new Map());
      expect(result).toBe(1);
      expect(queues.high.length).toBe(0);
    });

    it('유효한 작업은 유지해야 한다', () => {
      const queues = {
        high: [
          {
            id: 1,
            execute: jest.fn(),
            resolve: jest.fn(),
            reject: jest.fn(),
            timestamp: Date.now(),
            priority: 5,
          },
        ] as QueueTask[],
        normal: [] as QueueTask[],
        low: [] as QueueTask[],
      };

      const result = service.cleanupOldTasks(queues, new Map());
      expect(result).toBe(0);
      expect(queues.high.length).toBe(1);
    });

    it('만료된 배치도 정리해야 한다', () => {
      const batchQueues = new Map();
      batchQueues.set('test', {
        tasks: [
          {
            id: 1,
            execute: jest.fn(),
            resolve: jest.fn(),
            reject: jest.fn(),
            timestamp: Date.now() - 30000,
            priority: 0,
          },
        ],
        category: 'test',
        totalSize: 1,
        createdAt: Date.now() - 30000,
      });

      const result = service.cleanupOldTasks(
        { high: [], normal: [], low: [] },
        batchQueues,
      );
      expect(result).toBe(1);
      expect(batchQueues.size).toBe(0);
    });
  });
});
