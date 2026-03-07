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

describe('QueueService lifecycle and stats', () => {
  let service: QueueService;
  let processor: QueueProcessorService;
  let statsService: QueueStatsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: ALL_PROVIDERS,
    }).compile();

    service = module.get<QueueService>(QueueService);
    processor = module.get<QueueProcessorService>(QueueProcessorService);
    statsService = module.get<QueueStatsService>(QueueStatsService);
  });

  afterEach(async () => {
    await service.onModuleDestroy();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

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
