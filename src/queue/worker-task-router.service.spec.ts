import { Test, TestingModule } from '@nestjs/testing';
import { WorkerTaskRouterService } from './worker-task-router.service';
import {
  QueueTask,
  WorkerTaskData,
  WorkloadType,
} from './interfaces/queue-task.interface';

describe('WorkerTaskRouterService', () => {
  let router: WorkerTaskRouterService;

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

  const wrap = (task: QueueTask, extra: Partial<WorkerTaskData> = {}): WorkerTaskData => ({
    task,
    params: {},
    ...extra,
  });

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [WorkerTaskRouterService],
    }).compile();

    router = module.get<WorkerTaskRouterService>(WorkerTaskRouterService);
  });

  describe('determineTaskType', () => {
    it('taskData.type이 있으면 그대로 반환해야 한다', () => {
      const td = wrap(createTask(), { type: WorkloadType.MEMORY });
      expect(router.determineTaskType(td)).toBe(WorkloadType.MEMORY);
    });

    it('task.workloadType을 반영해야 한다', () => {
      expect(
        router.determineTaskType(wrap(createTask({ workloadType: WorkloadType.CPU }))),
      ).toBe(WorkloadType.CPU);
    });

    it('functionCode가 있으면 CUSTOM으로 분류해야 한다', () => {
      expect(
        router.determineTaskType(wrap(createTask({ functionCode: 'return 1;' }))),
      ).toBe(WorkloadType.CUSTOM);
    });

    it('taskData.functionCode도 CUSTOM으로 분류해야 한다', () => {
      expect(
        router.determineTaskType(wrap(createTask(), { functionCode: 'return 1;' })),
      ).toBe(WorkloadType.CUSTOM);
    });

    it('아무 힌트가 없으면 UNKNOWN을 반환해야 한다', () => {
      expect(router.determineTaskType(wrap(createTask()))).toBe(WorkloadType.UNKNOWN);
    });
  });

  describe('determineOperation', () => {
    it('taskData.operation이 있으면 그대로 반환해야 한다', () => {
      expect(
        router.determineOperation(wrap(createTask(), { operation: 'custom-op' })),
      ).toBe('custom-op');
    });

    it.each([
      ['prime-calc', 'findPrimes'],
      ['fibonacci-calc', 'fibonacci'],
      ['matrix-calc', 'matrixMultiply'],
      ['unknown-cpu', 'findPrimes'],
    ])('CPU 카테고리 "%s" → "%s"', (category, expected) => {
      expect(
        router.determineOperation(
          wrap(createTask({ category, workloadType: WorkloadType.CPU })),
        ),
      ).toBe(expected);
    });

    it.each([
      ['array-sort', 'largeArray'],
      ['object-clone', 'objectCloning'],
      ['unknown-mem', 'largeArray'],
    ])('MEMORY 카테고리 "%s" → "%s"', (category, expected) => {
      expect(
        router.determineOperation(
          wrap(createTask({ category, workloadType: WorkloadType.MEMORY })),
        ),
      ).toBe(expected);
    });

    it('CUSTOM이면 "execute"를 반환해야 한다', () => {
      expect(
        router.determineOperation(
          wrap(createTask({ workloadType: WorkloadType.CUSTOM })),
        ),
      ).toBe('execute');
    });
  });

  describe('isDispatchableType', () => {
    it('CPU, MEMORY, CUSTOM은 dispatchable이어야 한다', () => {
      expect(router.isDispatchableType(WorkloadType.CPU)).toBe(true);
      expect(router.isDispatchableType(WorkloadType.MEMORY)).toBe(true);
      expect(router.isDispatchableType(WorkloadType.CUSTOM)).toBe(true);
    });

    it('UNKNOWN은 dispatchable이 아니어야 한다', () => {
      expect(router.isDispatchableType(WorkloadType.UNKNOWN)).toBe(false);
    });
  });

  describe('canDispatchType / concurrency', () => {
    it('활성 수가 제한 미만이면 dispatch 가능해야 한다', () => {
      expect(router.canDispatchType(WorkloadType.CPU)).toBe(true);
    });

    it('활성 수가 제한에 도달하면 dispatch 불가해야 한다', () => {
      const limits = router.limits;
      for (let i = 0; i < limits[WorkloadType.CPU]; i++) {
        router.incrementActiveByType(WorkloadType.CPU);
      }
      expect(router.canDispatchType(WorkloadType.CPU)).toBe(false);
    });

    it('decrement 후 다시 dispatch 가능해야 한다', () => {
      const limits = router.limits;
      for (let i = 0; i < limits[WorkloadType.MEMORY]; i++) {
        router.incrementActiveByType(WorkloadType.MEMORY);
      }
      expect(router.canDispatchType(WorkloadType.MEMORY)).toBe(false);

      router.decrementActiveByType(WorkloadType.MEMORY);
      expect(router.canDispatchType(WorkloadType.MEMORY)).toBe(true);
    });

    it('decrementActiveByType는 0 아래로 내려가지 않아야 한다', () => {
      router.decrementActiveByType(WorkloadType.CPU);
      expect(router.getActiveByType()[WorkloadType.CPU]).toBe(0);
    });

    it('UNKNOWN 타입 decrement는 무시해야 한다', () => {
      router.decrementActiveByType(WorkloadType.UNKNOWN);
      expect(router.getActiveByType()[WorkloadType.CPU]).toBe(0);
    });
  });

  describe('dequeueDispatchableTask', () => {
    it('dispatchable 작업을 큐에서 꺼내야 한다', () => {
      const td = wrap(createTask({ workloadType: WorkloadType.CPU }));
      const queue = [td];
      const onFailed = jest.fn();

      const result = router.dequeueDispatchableTask(queue, onFailed);

      expect(result).not.toBeNull();
      expect(result!.type).toBe(WorkloadType.CPU);
      expect(queue).toHaveLength(0);
    });

    it('UNKNOWN 작업은 reject하고 건너뛰어야 한다', () => {
      const unknownTd = wrap(createTask());
      const cpuTd = wrap(createTask({ workloadType: WorkloadType.CPU }));
      const queue = [unknownTd, cpuTd];
      const onFailed = jest.fn();

      const result = router.dequeueDispatchableTask(queue, onFailed);

      expect(unknownTd.task.reject).toHaveBeenCalled();
      expect(onFailed).toHaveBeenCalledWith(unknownTd.task);
      expect(router.droppedUnknownWorkloadCount).toBe(1);
      expect(result!.type).toBe(WorkloadType.CPU);
      expect(queue).toHaveLength(0);
    });

    it('concurrency 제한에 걸린 타입은 건너뛰어야 한다', () => {
      const limits = router.limits;
      for (let i = 0; i < limits[WorkloadType.CPU]; i++) {
        router.incrementActiveByType(WorkloadType.CPU);
      }

      const cpuTd = wrap(createTask({ workloadType: WorkloadType.CPU }));
      const memTd = wrap(createTask({ workloadType: WorkloadType.MEMORY }));
      const queue = [cpuTd, memTd];
      const onFailed = jest.fn();

      const result = router.dequeueDispatchableTask(queue, onFailed);

      expect(result!.type).toBe(WorkloadType.MEMORY);
      expect(queue).toEqual([cpuTd]);
    });

    it('빈 큐이면 null을 반환해야 한다', () => {
      expect(router.dequeueDispatchableTask([], jest.fn())).toBeNull();
    });
  });

  describe('getPendingQueueByType', () => {
    it('타입별 대기 작업 수를 세야 한다', () => {
      const queue = [
        wrap(createTask({ workloadType: WorkloadType.CPU })),
        wrap(createTask({ workloadType: WorkloadType.CPU })),
        wrap(createTask({ workloadType: WorkloadType.MEMORY })),
        wrap(createTask()),
      ];

      const counts = router.getPendingQueueByType(queue);

      expect(counts[WorkloadType.CPU]).toBe(2);
      expect(counts[WorkloadType.MEMORY]).toBe(1);
      expect(counts[WorkloadType.UNKNOWN]).toBe(1);
      expect(counts[WorkloadType.CUSTOM]).toBe(0);
    });
  });

  describe('resetActiveByType', () => {
    it('모든 활성 카운트를 0으로 초기화해야 한다', () => {
      router.incrementActiveByType(WorkloadType.CPU);
      router.incrementActiveByType(WorkloadType.MEMORY);

      router.resetActiveByType();

      const active = router.getActiveByType();
      expect(active[WorkloadType.CPU]).toBe(0);
      expect(active[WorkloadType.MEMORY]).toBe(0);
      expect(active[WorkloadType.CUSTOM]).toBe(0);
    });
  });

  describe('getDispatchedByType', () => {
    it('누적 dispatch 횟수를 추적해야 한다', () => {
      router.incrementActiveByType(WorkloadType.CPU);
      router.incrementActiveByType(WorkloadType.CPU);
      router.incrementActiveByType(WorkloadType.MEMORY);

      const dispatched = router.getDispatchedByType();
      expect(dispatched[WorkloadType.CPU]).toBe(2);
      expect(dispatched[WorkloadType.MEMORY]).toBe(1);
    });
  });

  describe('limits', () => {
    it('타입별 concurrency 제한을 반환해야 한다', () => {
      const limits = router.limits;
      expect(limits[WorkloadType.CPU]).toBeGreaterThanOrEqual(1);
      expect(limits[WorkloadType.MEMORY]).toBeGreaterThanOrEqual(1);
      expect(limits[WorkloadType.CUSTOM]).toBeGreaterThanOrEqual(1);
    });
  });
});
