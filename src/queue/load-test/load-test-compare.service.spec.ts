import { LoadTestCompareService } from './load-test-compare.service';
import { EngineRouterService } from '../engine-router.service';
import { QueueService } from '../queue.service';
import { SimulationService } from '../simulation.service';

describe('LoadTestCompareService.compareEngines', () => {
  let queueService: QueueService;
  let engineRouter: EngineRouterService;
  let simulation: SimulationService;
  let service: LoadTestCompareService;

  beforeEach(() => {
    queueService = { enqueue: jest.fn() } as unknown as QueueService;
    engineRouter = {
      getEngine: jest.fn().mockReturnValue('both'),
      dispatchToGo: jest.fn(),
    } as unknown as EngineRouterService;
    simulation = {} as unknown as SimulationService;
    service = new LoadTestCompareService(
      queueService,
      engineRouter,
      simulation,
    );
  });

  it('both 모드가 아니면 안내를 돌려준다', async () => {
    (engineRouter.getEngine as jest.Mock).mockReturnValue('node');
    const res: any = await service.compareEngines({ count: 3 });
    expect(res.error).toMatch(/both/);
  });

  it('정상 비교에서는 양쪽 평균이 숫자로 나온다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockResolvedValue({
      success: true,
    });

    const res: any = await service.compareEngines({ count: 3 });

    expect(res.total).toBe(3);
    expect(res.errors).toBe(0);
    expect(typeof res.avgNodeMs).toBe('number');
    expect(typeof res.avgGoMs).toBe('number');
  });

  // Go 사이드카가 안 떠 있으면 전 라운드가 실패한다.
  // 이때 평균을 0 으로 내보내면 "양쪽 다 0ms, 가장 빠름"으로 읽혀
  // 전량 실패가 완벽한 성능처럼 보인다.
  it('전량 실패하면 평균 지연은 0 이 아니라 null 이다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockRejectedValue(
      new Error('Go 엔진 연결 실패'),
    );

    const res: any = await service.compareEngines({ count: 5 });

    expect(res.total).toBe(5);
    expect(res.errors).toBe(5);
    expect(res.avgNodeMs).toBeNull();
    expect(res.avgGoMs).toBeNull();
    expect(res.speedup).toBeNull();
  });

  it('실패 라운드의 지연은 0 이 아니라 null 로 기록된다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockRejectedValue(
      new Error('실패'),
    );

    const res: any = await service.compareEngines({ count: 2 });

    for (const round of res.detail) {
      expect(round.winner).toBe('error');
      expect(round.nodeMs).toBeNull();
      expect(round.goMs).toBeNull();
    }
  });

  it('일부만 실패하면 성공 라운드만으로 평균을 낸다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    let call = 0;
    (engineRouter.dispatchToGo as jest.Mock).mockImplementation(async () => {
      call += 1;
      if (call === 2) throw new Error('두 번째만 실패');
      return { success: true };
    });

    const res: any = await service.compareEngines({ count: 3 });

    expect(res.errors).toBe(1);
    expect(res.avgNodeMs).not.toBeNull();
    expect(res.avgGoMs).not.toBeNull();
  });
});

describe('LoadTestCompareService.compareStream', () => {
  let queueService: QueueService;
  let engineRouter: EngineRouterService;
  let simulation: SimulationService;
  let service: LoadTestCompareService;

  beforeEach(() => {
    queueService = { enqueue: jest.fn() } as unknown as QueueService;
    engineRouter = {
      getEngine: jest.fn().mockReturnValue('both'),
      dispatchToGo: jest.fn(),
    } as unknown as EngineRouterService;
    simulation = { simulateIO: jest.fn() } as unknown as SimulationService;
    service = new LoadTestCompareService(
      queueService,
      engineRouter,
      simulation,
    );
  });

  /** 스트림이 끝날 때까지 이벤트를 모은다. */
  function collect(
    stream: ReturnType<LoadTestCompareService['compareStream']>,
  ): Promise<Record<string, any>[]> {
    return new Promise((resolve, reject) => {
      const events: Record<string, any>[] = [];
      stream.subscribe({
        next: (message: any) => events.push(JSON.parse(message.data)),
        error: reject,
        complete: () => resolve(events),
      });
    });
  }

  const eventsOf = (events: Record<string, any>[], name: string) =>
    events.filter((event) => event.event === name);

  it('both 모드가 아니면 error 이벤트를 내고 끝낸다', async () => {
    (engineRouter.getEngine as jest.Mock).mockReturnValue('node');

    const events = await collect(service.compareStream({ count: 3 }));

    expect(eventsOf(events, 'error')[0].message).toMatch(/both/);
    // 비교가 성립하지 않으므로 라운드를 돌면 안 된다.
    expect(eventsOf(events, 'progress')).toHaveLength(0);
    expect(eventsOf(events, 'done')).toHaveLength(0);
  });

  it('start 로 시작해 라운드마다 progress 를 내고 done 으로 끝낸다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockResolvedValue({
      success: true,
    });

    const events = await collect(service.compareStream({ count: 3 }));

    expect(events[0].event).toBe('start');
    expect(events[0].count).toBe(3);
    expect(eventsOf(events, 'progress')).toHaveLength(3);
    expect(events[events.length - 1].event).toBe('done');
    expect(eventsOf(events, 'done')[0].total).toBe(3);
    expect(eventsOf(events, 'done')[0].errors).toBe(0);
  });

  // Go 사이드카가 안 떠 있으면 전 라운드가 실패한다. 이때 평균을 0 으로
  // 내보내면 "양쪽 다 0ms" 로 읽혀 전량 실패가 완벽한 성능처럼 보인다.
  it('전량 실패하면 done 의 평균 지연은 0 이 아니라 null 이다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockRejectedValue(
      new Error('Go 엔진 연결 실패'),
    );

    const events = await collect(service.compareStream({ count: 4 }));
    const done = eventsOf(events, 'done')[0];

    expect(done.errors).toBe(4);
    expect(done.node.avgMs).toBeNull();
    expect(done.go.avgMs).toBeNull();
  });

  it('실패 라운드의 progress 는 지연을 null 로 보고한다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockRejectedValue(
      new Error('실패'),
    );

    const events = await collect(service.compareStream({ count: 2 }));

    for (const progress of eventsOf(events, 'progress')) {
      expect(progress.winner).toBe('error');
      expect(progress.nodeMs).toBeNull();
      expect(progress.goMs).toBeNull();
      // 누적 평균도 표본이 없으면 null 이어야 한다.
      expect(progress.avgNodeMs).toBeNull();
      expect(progress.avgGoMs).toBeNull();
    }
  });

  it('일부만 실패하면 성공 라운드만으로 집계한다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    let call = 0;
    (engineRouter.dispatchToGo as jest.Mock).mockImplementation(async () => {
      call += 1;
      if (call === 2) throw new Error('두 번째만 실패');
      return { success: true };
    });

    const done = eventsOf(
      await collect(service.compareStream({ count: 3 })),
      'done',
    )[0];

    expect(done.errors).toBe(1);
    expect(done.nodeWins + done.goWins).toBe(2);
    expect(done.node.avgMs).not.toBeNull();
    expect(done.go.avgMs).not.toBeNull();
  });

  it('io 모드는 simulateIO 를 쓰고 done 에 그 사실을 남긴다', async () => {
    (simulation.simulateIO as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockResolvedValue({
      success: true,
    });

    const done = eventsOf(
      await collect(
        service.compareStream({ count: 2, testType: 'io', delayMs: 5 }),
      ),
      'done',
    )[0];

    expect(simulation.simulateIO).toHaveBeenCalledWith(5);
    expect(queueService.enqueue).not.toHaveBeenCalled();
    expect(done.testType).toBe('io');
    expect(done.task).toMatch(/asyncIO/);
  });

  it('요청 건수는 200 건으로 제한한다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockResolvedValue({
      success: true,
    });

    const events = await collect(service.compareStream({ count: 500 }));

    expect(events[0].count).toBe(200);
  });
});
