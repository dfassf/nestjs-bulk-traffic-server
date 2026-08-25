import { LoadTestRunnerService } from './load-test-runner.service';
import { QueueService } from '../queue.service';
import { SimulationService } from '../simulation.service';
import { BenchDriver } from '../bench-driver.interface';

describe('LoadTestRunnerService.runStream', () => {
  let queueService: QueueService;
  let simulation: SimulationService;
  let bench: BenchDriver;
  let service: LoadTestRunnerService;

  beforeEach(() => {
    queueService = {
      enqueue: jest.fn().mockResolvedValue(undefined),
    } as unknown as QueueService;
    simulation = {
      simulateCPU: jest.fn(),
      simulateIO: jest.fn(),
    } as unknown as SimulationService;
    bench = {
      benchWrite: jest.fn().mockResolvedValue({}),
      benchRead: jest.fn().mockResolvedValue({}),
    } as unknown as BenchDriver;
    service = new LoadTestRunnerService(queueService, simulation, bench);
  });

  /** 스트림이 끝날 때까지 이벤트를 모은다. */
  function collect(
    stream: ReturnType<LoadTestRunnerService['runStream']>,
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

  // 구독하기 전에 발행된 이벤트는 사라진다. start 를 놓치면 화면이
  // 언제 시작했는지 모른 채로 progress 만 받게 된다.
  it('첫 이벤트인 start 부터 빠짐없이 받는다', async () => {
    const events = await collect(service.runStream({ type: 'cpu', count: 3 }));

    expect(events[0].event).toBe('start');
    expect(events[0].count).toBe(3);
    expect(events[0].type).toBe('cpu');
  });

  it('라운드마다 progress 를 내고 done 으로 끝낸다', async () => {
    const events = await collect(service.runStream({ type: 'cpu', count: 4 }));

    expect(eventsOf(events, 'progress')).toHaveLength(4);
    expect(events[events.length - 1].event).toBe('done');
    expect(eventsOf(events, 'done')[0].total).toBe(4);
  });

  it('작업이 실패해도 라운드를 계속 돌고 실패로 센다', async () => {
    (queueService.enqueue as jest.Mock).mockRejectedValue(new Error('실패'));

    const events = await collect(service.runStream({ type: 'cpu', count: 3 }));
    const done = eventsOf(events, 'done')[0];

    expect(eventsOf(events, 'progress')).toHaveLength(3);
    expect(done.rejected).toBe(3);
    expect(done.fulfilled).toBe(0);
  });

  // 전량 실패인데 지연을 0 으로 내보내면 가장 빠른 결과처럼 보인다.
  it('전량 실패하면 지연 지표는 0 이 아니라 null 이다', async () => {
    (queueService.enqueue as jest.Mock).mockRejectedValue(new Error('실패'));

    const done = eventsOf(
      await collect(service.runStream({ type: 'cpu', count: 3 })),
      'done',
    )[0];

    expect(done.avgMs).toBeNull();
    expect(done.p50).toBeNull();
    expect(done.p95).toBeNull();
  });

  it('일부만 실패하면 성공·실패를 나눠 센다', async () => {
    let call = 0;
    (queueService.enqueue as jest.Mock).mockImplementation(async () => {
      call += 1;
      if (call === 2) throw new Error('두 번째만 실패');
    });

    const done = eventsOf(
      await collect(service.runStream({ type: 'cpu', count: 3 })),
      'done',
    )[0];

    expect(done.fulfilled).toBe(2);
    expect(done.rejected).toBe(1);
    expect(done.avgMs).not.toBeNull();
  });

  it('db-write 는 벤치 드라이버를 쓴다', async () => {
    await collect(service.runStream({ type: 'db-write', count: 2 }));

    expect(bench.benchWrite).toHaveBeenCalledTimes(2);
    expect(queueService.enqueue).not.toHaveBeenCalled();
  });

  it('요청 건수는 500 건으로 제한한다', async () => {
    const events = await collect(
      service.runStream({ type: 'cpu', count: 9999 }),
    );

    expect(events[0].count).toBe(500);
  });
});
