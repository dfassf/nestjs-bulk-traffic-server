import { QueueStatsController } from './queue-stats.controller';
import { QueueService } from '../queue.service';
import { EngineRouterService } from '../engine-router.service';
import { GoEngineClient } from '../go-engine.client';

describe('QueueStatsController', () => {
  let queueService: QueueService;
  let engineRouter: EngineRouterService;
  let goEngineClient: GoEngineClient;
  let controller: QueueStatsController;

  beforeEach(() => {
    queueService = {
      getQueueStats: jest.fn().mockReturnValue({
        totalQueueLength: 3,
        memoryPressure: false,
      }),
    } as unknown as QueueService;

    engineRouter = {
      getEngine: jest.fn().mockReturnValue('node'),
      isBackendWired: jest.fn().mockReturnValue(false),
    } as unknown as EngineRouterService;

    goEngineClient = {
      isConnected: jest.fn().mockReturnValue(false),
      getStats: jest.fn(),
    } as unknown as GoEngineClient;

    controller = new QueueStatsController(
      queueService,
      engineRouter,
      goEngineClient,
    );
  });

  describe('queue-stats', () => {
    it('큐 통계를 그대로 펼쳐서 담는다', () => {
      const res: any = controller.getStats();
      expect(res.totalQueueLength).toBe(3);
      expect(res.memoryPressure).toBe(false);
      expect(typeof res.timestamp).toBe('string');
      expect(typeof res.uptime).toBe('number');
    });

    it('엔진 모드를 보고한다', () => {
      (engineRouter.getEngine as jest.Mock).mockReturnValue('kafka');
      const res: any = controller.getStats();
      expect(res.engineMode).toBe('kafka');
    });

    // 엔진 모드만 보면 그 백엔드로 작업이 나가는 줄로 읽힌다.
    // 배선 여부를 같이 알려야 상태 응답이 실제와 어긋나지 않는다.
    it('엔진 모드와 별개로 백엔드 배선 여부를 함께 보고한다', () => {
      (engineRouter.getEngine as jest.Mock).mockReturnValue('kafka');
      (engineRouter.isBackendWired as jest.Mock).mockReturnValue(false);

      const res: any = controller.getStats();

      expect(res.engineMode).toBe('kafka');
      expect(res.engineBackendWired).toBe(false);
    });

    it('배선이 완료되면 true 로 보고한다', () => {
      (engineRouter.isBackendWired as jest.Mock).mockReturnValue(true);
      const res: any = controller.getStats();
      expect(res.engineBackendWired).toBe(true);
    });

    it('Go 엔진 연결 상태를 보고한다', () => {
      (goEngineClient.isConnected as jest.Mock).mockReturnValue(true);
      const res: any = controller.getStats();
      expect(res.goEngineConnected).toBe(true);
    });

    it('메모리 사용량을 MB 단위로 담는다', () => {
      const res: any = controller.getStats();
      expect(typeof res.memory.heapUsed).toBe('number');
      expect(typeof res.memory.heapTotal).toBe('number');
      expect(typeof res.memory.rss).toBe('number');
      expect(res.memory.heapUsed).toBeGreaterThan(0);
    });
  });

  describe('go-engine-stats', () => {
    it('조회 성공 시 connected=true 와 통계를 함께 돌려준다', async () => {
      (goEngineClient.getStats as jest.Mock).mockResolvedValue({
        totalProcessed: 10,
        totalFailed: 1,
      });

      const res: any = await controller.getGoEngineStats();

      expect(res.connected).toBe(true);
      expect(res.totalProcessed).toBe(10);
    });

    // 조회 실패를 빈 통계로 바꾸면 "처리 0건"과 구분이 안 된다.
    it('조회 실패는 connected=false 와 오류 메시지로 드러낸다', async () => {
      (goEngineClient.getStats as jest.Mock).mockRejectedValue(
        new Error('연결 끊김'),
      );

      const res: any = await controller.getGoEngineStats();

      expect(res.connected).toBe(false);
      expect(res.error).toBe('연결 끊김');
      expect(res.totalProcessed).toBeUndefined();
    });
  });
});
