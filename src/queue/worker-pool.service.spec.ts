import { Test, TestingModule } from '@nestjs/testing';
import { WorkerPoolService } from './worker-pool.service';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';

describe('WorkerPoolService', () => {
  const originalDisableWorkers = process.env.DISABLE_WORKERS;

  const createTask = (overrides: Partial<QueueTask> = {}): QueueTask => ({
    id: 1,
    execute: jest.fn().mockResolvedValue('result'),
    resolve: jest.fn(),
    reject: jest.fn(),
    timestamp: Date.now(),
    priority: 0,
    category: 'test',
    ...overrides,
  });

  const createService = async (): Promise<WorkerPoolService> => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [WorkerPoolService],
    }).compile();

    return module.get<WorkerPoolService>(WorkerPoolService);
  };

  beforeEach(() => {
    process.env.DISABLE_WORKERS = 'true';
  });

  afterEach(async () => {
    process.env.DISABLE_WORKERS = originalDisableWorkers;
    jest.restoreAllMocks();
  });

  it('DISABLE_WORKERS=true면 워커가 비활성화되어야 한다', async () => {
    const service = await createService();
    expect(service.isEnabled).toBe(false);
  });

  it('작업 큐에 태스크를 추가할 수 있어야 한다', async () => {
    const service = await createService();
    const taskData: WorkerTaskData = {
      task: createTask(),
      params: {},
    };

    service.addTask(taskData);

    expect(service.taskQueue).toHaveLength(1);
    expect(service.taskQueue[0]).toBe(taskData);
  });

  it('workloadType 기반으로 작업 유형을 판별해야 한다', async () => {
    const service = await createService();

    const cpuType = service.determineTaskType({
      task: createTask({ workloadType: WorkloadType.CPU }),
      params: {},
    });
    const memoryType = service.determineTaskType({
      task: createTask({ workloadType: WorkloadType.MEMORY }),
      params: {},
    });
    const customType = service.determineTaskType({
      task: createTask({ workloadType: WorkloadType.CUSTOM }),
      params: {},
    });

    expect(cpuType).toBe(WorkloadType.CPU);
    expect(memoryType).toBe(WorkloadType.MEMORY);
    expect(customType).toBe(WorkloadType.CUSTOM);
  });

  it('workloadType이 없으면 functionCode만 custom으로 해석하고 나머지는 unknown으로 분류해야 한다', async () => {
    const service = await createService();

    const customType = service.determineTaskType({
      task: createTask({ functionCode: 'return params;' }),
      params: {},
    });
    const unknownType = service.determineTaskType({
      task: createTask(),
      params: {},
    });

    expect(customType).toBe(WorkloadType.CUSTOM);
    expect(unknownType).toBe(WorkloadType.UNKNOWN);
  });

  it('카테고리 기반 operation을 판별해야 한다', async () => {
    const service = await createService();

    expect(
      service.determineOperation({
        task: createTask({
          category: 'prime-calc',
          workloadType: WorkloadType.CPU,
        }),
        params: {},
      }),
    ).toBe('findPrimes');

    expect(
      service.determineOperation({
        task: createTask({
          category: 'fibonacci-calc',
          workloadType: WorkloadType.CPU,
        }),
        params: {},
      }),
    ).toBe('fibonacci');

    expect(
      service.determineOperation({
        task: createTask({
          category: 'matrix-calc',
          workloadType: WorkloadType.CPU,
        }),
        params: {},
      }),
    ).toBe('matrixMultiply');
  });

  it('타입별 동시성 제한을 넘기면 dispatch가 차단되어야 한다', async () => {
    const service = await createService();
    const internal = service as any;

    internal.activeByType[WorkloadType.MEMORY] = internal.memoryConcurrencyLimit;

    expect(internal.canDispatchType(WorkloadType.MEMORY)).toBe(false);
    expect(internal.canDispatchType(WorkloadType.CPU)).toBe(true);
  });

  it('워커 없이 큐 작업을 처리할 수 있어야 한다', async () => {
    const service = await createService();

    const taskData: WorkerTaskData = {
      task: createTask({ execute: jest.fn().mockResolvedValue('ok') }),
      params: {},
    };

    service.addTask(taskData);

    const onSuccess = jest.fn();
    const onError = jest.fn();

    await service.processWithoutWorkers(onSuccess, onError);

    expect(taskData.task.resolve).toHaveBeenCalledWith('ok');
    expect(onSuccess).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });

  it('워커 결과 성공/실패를 올바르게 반영해야 한다', async () => {
    const service = await createService();

    const successTaskData: WorkerTaskData = {
      task: createTask(),
      params: {},
    };

    (service as any).assignedTasks.set(0, successTaskData);
    (service as any).workerBusy[0] = true;

    const onSuccess = jest.fn();
    const onFailed = jest.fn();

    service.handleWorkerResult(
      0,
      { success: true, result: 'done' },
      onSuccess,
      onFailed,
    );

    expect(successTaskData.task.resolve).toHaveBeenCalledWith('done');
    expect(onSuccess).toHaveBeenCalledWith(successTaskData.task);
    expect(onFailed).not.toHaveBeenCalled();

    const failedTaskData: WorkerTaskData = {
      task: createTask(),
      params: {},
    };

    (service as any).assignedTasks.set(1, failedTaskData);
    (service as any).workerBusy[1] = true;

    service.handleWorkerResult(
      1,
      { success: false, error: 'boom' },
      onSuccess,
      onFailed,
    );

    expect(failedTaskData.task.reject).toHaveBeenCalled();
    expect(onFailed).toHaveBeenCalledWith(failedTaskData.task);
  });
});
