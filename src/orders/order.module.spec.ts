import { Test } from '@nestjs/testing';
import { OrderModule } from './order.module';
import { OrderService } from './order.service';
import { OrderPublisher } from './order-publisher';
import { OrderTopicProvisioner } from './order-topic.provisioner';
import { ConsumerProcessManager } from './lab/consumer-process.manager';
import { KafkaInspector } from './lab/kafka-inspector';
import { OrderController } from './order.controller';
import { LabController } from './lab/lab.controller';
import { ORDER_STORE } from './order-store.interface';

/**
 * 모듈이 실제로 조립되는지 검증한다.
 *
 * 각 클래스를 new 로 직접 만드는 단위 테스트는 DI 배선 오류를 못 잡는다.
 * 실제로 ConsumerProcessManager 의 선택적 생성자 인자(string) 때문에
 * Nest 가 부팅에 실패한 적이 있는데, 단위 테스트는 전부 통과했다.
 */
describe('OrderModule 조립', () => {
  const keys = ['WORKER_ENGINE', 'ORDER_DB_PATH', 'KAFKA_BROKERS'];
  const saved: Record<string, string | undefined> = {};

  beforeAll(() => {
    keys.forEach((k) => (saved[k] = process.env[k]));
    // 카프카에 실제로 붙지 않도록 kafka 모드를 끈다.
    // 이 테스트가 보려는 건 배선이지 통신이 아니다.
    process.env.WORKER_ENGINE = 'node';
    process.env.ORDER_DB_PATH = ':memory:';
  });

  afterAll(() => {
    keys.forEach((k) => {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k]!;
    });
  });

  async function build() {
    return Test.createTestingModule({ imports: [OrderModule] }).compile();
  }

  it('모듈이 오류 없이 조립된다', async () => {
    const moduleRef = await build();
    expect(moduleRef).toBeDefined();
    await moduleRef.close();
  });

  it.each([
    ['OrderService', OrderService],
    ['OrderPublisher', OrderPublisher],
    ['OrderTopicProvisioner', OrderTopicProvisioner],
    ['ConsumerProcessManager', ConsumerProcessManager],
    ['KafkaInspector', KafkaInspector],
  ])('%s 를 주입받을 수 있다', async (_name, token) => {
    const moduleRef = await build();
    expect(moduleRef.get(token as any)).toBeDefined();
    await moduleRef.close();
  });

  it.each([
    ['OrderController', OrderController],
    ['LabController', LabController],
  ])('%s 가 등록된다', async (_name, token) => {
    const moduleRef = await build();
    expect(moduleRef.get(token as any)).toBeDefined();
    await moduleRef.close();
  });

  it('주문 저장소가 초기화된 상태로 주입된다', async () => {
    const moduleRef = await build();
    const store = moduleRef.get(ORDER_STORE);

    expect(store).toBeDefined();
    // init 이 끝나야 조회가 된다. 팩토리가 await 를 빠뜨리면 여기서 터진다.
    await expect(store.countOrders()).resolves.toBeGreaterThanOrEqual(0);

    await moduleRef.close();
  });

  // 선택적 생성자 인자에 @Optional() 이 빠지면 Nest 가 주입을 시도하다 실패한다.
  it('컨슈머 스크립트 경로 없이도 프로세스 관리자가 만들어진다', async () => {
    const moduleRef = await build();
    const manager = moduleRef.get(ConsumerProcessManager);

    expect(manager.list()).toEqual([]);
    await moduleRef.close();
  });
});
