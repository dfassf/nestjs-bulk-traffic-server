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
    service = new LoadTestCompareService(queueService, engineRouter, simulation);
  });

  it('both 모드가 아니면 안내를 돌려준다', async () => {
    (engineRouter.getEngine as jest.Mock).mockReturnValue('node');
    const res: any = await service.compareEngines({ count: 3 });
    expect(res.error).toMatch(/both/);
  });

  it('정상 비교에서는 양쪽 평균이 숫자로 나온다', async () => {
    (queueService.enqueue as jest.Mock).mockResolvedValue(undefined);
    (engineRouter.dispatchToGo as jest.Mock).mockResolvedValue({ success: true });

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
    (engineRouter.dispatchToGo as jest.Mock).mockRejectedValue(new Error('실패'));

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
