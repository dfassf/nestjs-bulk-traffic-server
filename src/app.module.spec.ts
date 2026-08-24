import { MiddlewareConsumer, RequestMethod } from '@nestjs/common';
import { AppModule } from './app.module';
import { QueueMiddleware } from './queue/middleware/queue.middleware';

interface ExcludedRoute {
  path: string;
  method: RequestMethod;
}

/**
 * 큐 미들웨어 등록 설정 검증.
 *
 * 관측·실험용 경로가 제외 목록에서 빠지면 그 요청이 큐에 쌓인다.
 * 부하 상황에서 상태를 보려고 부르는 API 가 그 부하 때문에 막히고,
 * 주문 발행은 지연이 큐 때문인지 카프카 때문인지 구분이 안 된다.
 *
 * 특히 주문 경로는 queue-request-analyzer 의 shouldBypass 에는 없어서
 * 이 exclude 가 유일한 보호 지점이다.
 */
describe('AppModule 미들웨어 등록', () => {
  let excluded: ExcludedRoute[];
  let appliedMiddleware: unknown[];
  let forRoutesArg: unknown;

  beforeEach(() => {
    excluded = [];
    appliedMiddleware = [];
    forRoutesArg = undefined;

    const chain = {
      exclude: (...routes: ExcludedRoute[]) => {
        excluded = routes;
        return chain;
      },
      forRoutes: (arg: unknown) => {
        forRoutesArg = arg;
        return chain;
      },
    };

    const consumer = {
      apply: (...middleware: unknown[]) => {
        appliedMiddleware = middleware;
        return chain;
      },
    } as unknown as MiddlewareConsumer;

    new AppModule().configure(consumer);
  });

  it('큐 미들웨어를 모든 경로에 적용한다', () => {
    expect(appliedMiddleware).toContain(QueueMiddleware);
    expect(forRoutesArg).toEqual({ path: '*', method: RequestMethod.ALL });
  });

  it.each([
    ['health', '헬스체크'],
    ['queue-stats', '큐 상태 조회'],
    ['benchmark-stats', '벤치 통계'],
    ['go-engine-stats', 'Go 엔진 통계'],
  ])('관측 경로 %s(%s)는 큐를 거치지 않는다', (path) => {
    expect(excluded.some((route) => route.path === path)).toBe(true);
  });

  it('부하 테스트 하위 경로는 큐를 거치지 않는다', () => {
    expect(excluded.some((route) => route.path === 'load-test/(.*)')).toBe(
      true,
    );
  });

  // 주문 API 가 큐를 타면 카프카 실험 결과를 읽을 수 없다.
  // 단건 경로와 하위 경로를 둘 다 빼야 /orders 와 /orders/bulk 가 모두 통과한다.
  it('주문 단건 경로는 큐를 거치지 않는다', () => {
    expect(excluded.some((route) => route.path === 'orders')).toBe(true);
  });

  it('주문 하위 경로도 큐를 거치지 않는다', () => {
    expect(excluded.some((route) => route.path === 'orders/(.*)')).toBe(true);
  });

  // 부하를 거는 도중에 컨슈머를 죽이거나 Lag 을 봐야 하는데,
  // 조작판이 큐에 막히면 실험 자체를 진행할 수 없다.
  it('실험 조작판은 큐를 거치지 않는다', () => {
    expect(excluded.some((route) => route.path === 'lab/(.*)')).toBe(true);
  });

  it('제외 경로는 모든 메서드를 대상으로 한다', () => {
    for (const route of excluded) {
      expect(route.method).toBe(RequestMethod.ALL);
    }
  });

  // 일반 업무 경로까지 빠지면 부하 제어가 통째로 무력해진다.
  it('일반 경로는 제외 목록에 없다', () => {
    const paths = excluded.map((route) => route.path);
    expect(paths).not.toContain('*');
    expect(paths).not.toContain('api');
  });
});
