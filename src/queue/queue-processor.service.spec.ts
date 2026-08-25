import { QueueProcessorService } from './queue-processor.service';
import { QueueStateHolder } from './queue-state.holder';
import { QueueStatsService } from './queue-stats.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { WorkerPoolService } from './worker-pool.service';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

/**
 * 상태·통계는 실물을 쓴다. 둘 다 값을 담아두는 객체라 가짜로 바꾸면
 * "정말 세어졌는지" 가 아니라 "부르긴 했는지" 만 확인하게 된다.
 */
describe('QueueProcessorService.processQueue', () => {
  let state: QueueStateHolder;
  let stats: QueueStatsService;
  let memoryService: MemoryService;
  let batchService: BatchService;
  let workerPoolService: WorkerPoolService;
  let service: QueueProcessorService;

  beforeEach(() => {
    state = new QueueStateHolder();
    stats = new QueueStatsService();
    memoryService = { memoryPressure: false } as unknown as MemoryService;
    batchService = {} as unknown as BatchService;
    // 워커 풀을 꺼서 인라인 실행 경로를 타게 한다.
    workerPoolService = {
      isEnabled: false,
      addTask: jest.fn(),
      processWorkerTasks: jest.fn(),
      handleWorkerResult: jest.fn(),
    } as unknown as WorkerPoolService;

    service = new QueueProcessorService(
      state,
      memoryService,
      batchService,
      workerPoolService,
      stats,
    );
  });

  function enqueueTask(execute: () => Promise<unknown>): {
    resolved: unknown[];
    rejected: unknown[];
  } {
    const resolved: unknown[] = [];
    const rejected: unknown[] = [];

    state.normalPriorityQueue.push({
      id: state.taskIdCounter++,
      execute,
      resolve: (value: unknown) => resolved.push(value),
      reject: (error: unknown) => rejected.push(error),
      priority: 0,
      workloadType: WorkloadType.CPU,
      params: {},
      createdAt: Date.now(),
    } as unknown as QueueTask);

    return { resolved, rejected };
  }

  /** 인라인 실행은 프라미스 체인이라 마이크로태스크가 다 돌 때까지 기다린다. */
  const flush = () => new Promise((resolve) => setImmediate(resolve));

  afterEach(async () => {
    // processQueue 는 큐가 남아 있으면 다음 회차를 setImmediate 로 예약한다.
    // 그대로 두면 예약된 타이머가 남아 테스트 러너가 끝나지 않는다.
    state.highPriorityQueue.length = 0;
    state.normalPriorityQueue.length = 0;
    state.lowPriorityQueue.length = 0;
    await flush();
  });

  it('성공한 작업은 처리 건수로 센다', async () => {
    const task = enqueueTask(async () => 'ok');

    service.processQueue();
    await flush();

    expect(task.resolved).toEqual(['ok']);
    expect(stats.totalProcessed).toBe(1);
    expect(stats.totalRejected).toBe(0);
  });

  // 실패를 세지 않으면 대시보드에 실패가 0 으로 남는다. 작업이 터지고
  // 있는데 화면은 아무 문제 없는 것처럼 보인다.
  it('실패한 작업은 실패 건수로 센다', async () => {
    const boom = new Error('작업 실패');
    const task = enqueueTask(async () => {
      throw boom;
    });

    service.processQueue();
    await flush();

    expect(task.rejected).toEqual([boom]);
    expect(stats.totalRejected).toBe(1);
    expect(stats.totalProcessed).toBe(0);
  });

  it('성공과 실패가 섞이면 각각 세어 합이 맞는다', async () => {
    enqueueTask(async () => 'ok');
    enqueueTask(async () => {
      throw new Error('실패');
    });
    enqueueTask(async () => 'ok');

    service.processQueue();
    await flush();

    expect(stats.totalProcessed).toBe(2);
    expect(stats.totalRejected).toBe(1);
    // 어느 쪽으로든 반드시 한 번은 세어져야 한다. 빠지면 합이 안 맞는다.
    expect(stats.totalProcessed + stats.totalRejected).toBe(3);
  });

  it('실패해도 동시 실행 수를 되돌린다', async () => {
    enqueueTask(async () => {
      throw new Error('실패');
    });

    service.processQueue();
    await flush();

    // 안 되돌리면 슬롯이 잠겨 이후 작업이 영영 처리되지 않는다.
    expect(state.activeRequests).toBe(0);
  });

  it('우선순위가 높은 큐를 먼저 처리한다', async () => {
    const order: string[] = [];
    state.highPriorityQueue.push({
      id: 1,
      execute: async () => order.push('high'),
      resolve: () => undefined,
      reject: () => undefined,
      priority: 10,
      workloadType: WorkloadType.CPU,
      params: {},
      createdAt: Date.now(),
    } as unknown as QueueTask);
    enqueueTask(async () => order.push('normal'));

    service.processQueue();
    await flush();

    expect(order[0]).toBe('high');
  });

  // 메모리가 부족하면 낮은 우선순위는 이번 회차에서 건너뛴다.
  it('메모리 압박 중에는 저우선순위 큐를 건드리지 않는다', async () => {
    (memoryService as { memoryPressure: boolean }).memoryPressure = true;
    const ran: string[] = [];
    state.lowPriorityQueue.push({
      id: 1,
      execute: async () => ran.push('low'),
      resolve: () => undefined,
      reject: () => undefined,
      priority: -1,
      workloadType: WorkloadType.CPU,
      params: {},
      createdAt: Date.now(),
    } as unknown as QueueTask);

    service.processQueue();
    await flush();

    expect(ran).toHaveLength(0);
    expect(state.lowPriorityQueue).toHaveLength(1);
  });
});
