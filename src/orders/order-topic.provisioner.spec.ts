import {
  OrderTopicProvisioner,
  TopicProvisionerConfig,
  topicProvisionerConfigFromEnv,
} from './order-topic.provisioner';
import { ORDER_TOPICS } from './order-events';

jest.mock('kafkajs', () => {
  const listTopicsMock = jest.fn();
  const createTopicsMock = jest.fn();
  const fetchTopicMetadataMock = jest.fn();
  const connectMock = jest.fn();
  const disconnectMock = jest.fn();
  const adminCtor = jest.fn(() => ({
    connect: connectMock,
    disconnect: disconnectMock,
    listTopics: listTopicsMock,
    createTopics: createTopicsMock,
    fetchTopicMetadata: fetchTopicMetadataMock,
  }));
  const kafkaCtor = jest.fn(() => ({ admin: adminCtor }));
  return {
    Kafka: kafkaCtor,
    logLevel: { NOTHING: 0 },
    __mocks: {
      listTopicsMock,
      createTopicsMock,
      fetchTopicMetadataMock,
      connectMock,
      disconnectMock,
      kafkaCtor,
    },
  };
});

const mocks = () => (jest.requireMock('kafkajs') as any).__mocks;
const ALL_TOPICS = Object.values(ORDER_TOPICS);

function buildConfig(
  overrides: Partial<TopicProvisionerConfig> = {},
): TopicProvisionerConfig {
  return {
    brokers: ['localhost:9092'],
    clientId: 'test-admin',
    enabled: true,
    partitions: 6,
    replicationFactor: 1,
    ...overrides,
  };
}

/** 메타데이터 응답을 만든다. partitionCounts 에 없는 토픽은 기대 파티션 수를 쓴다. */
function stubMetadata(partitionCounts: Record<string, number>, fallback = 6) {
  mocks().fetchTopicMetadataMock.mockImplementation(
    async ({ topics }: any) => ({
      topics: topics.map((name: string) => ({
        name,
        partitions: Array.from(
          { length: partitionCounts[name] ?? fallback },
          (_, i) => ({
            partitionId: i,
          }),
        ),
      })),
    }),
  );
}

describe('topicProvisionerConfigFromEnv', () => {
  const keys = [
    'ORDER_TOPIC_PARTITIONS',
    'ORDER_TOPIC_REPLICATION_FACTOR',
    'ORDER_TOPIC_ADMIN_CLIENT_ID',
  ];
  afterEach(() => keys.forEach((k) => delete process.env[k]));

  it('기본 파티션은 6개다', () => {
    expect(topicProvisionerConfigFromEnv(true).partitions).toBe(6);
  });

  it('파티션 수를 환경변수로 바꾼다', () => {
    process.env.ORDER_TOPIC_PARTITIONS = '12';
    expect(topicProvisionerConfigFromEnv(true).partitions).toBe(12);
  });

  it('단일 노드 기본 복제 팩터는 1이다', () => {
    expect(topicProvisionerConfigFromEnv(true).replicationFactor).toBe(1);
  });
});

describe('OrderTopicProvisioner', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    stubMetadata({});
  });

  it('꺼져 있으면 카프카에 연결하지 않는다', async () => {
    const provisioner = new OrderTopicProvisioner(
      buildConfig({ enabled: false }),
    );
    await provisioner.onModuleInit();

    expect(mocks().kafkaCtor).not.toHaveBeenCalled();
    expect(provisioner.getLastReport()).toBeNull();
  });

  it('브로커 주소가 비면 예외를 던진다', async () => {
    const provisioner = new OrderTopicProvisioner(buildConfig({ brokers: [] }));
    await expect(provisioner.onModuleInit()).rejects.toThrow(
      /브로커 주소가 비어/,
    );
  });

  describe('토픽 생성', () => {
    it('없는 토픽을 지정한 파티션 수로 만든다', async () => {
      mocks().listTopicsMock.mockResolvedValue([]);
      const provisioner = new OrderTopicProvisioner(
        buildConfig({ partitions: 6 }),
      );

      const report = await provisioner.provision();

      expect(mocks().createTopicsMock).toHaveBeenCalledWith(
        expect.objectContaining({
          waitForLeaders: true,
          topics: ALL_TOPICS.map((topic) => ({
            topic,
            numPartitions: 6,
            replicationFactor: 1,
          })),
        }),
      );
      expect(report.created).toEqual(ALL_TOPICS);
      expect(report.existing).toEqual([]);
    });

    it('이미 있는 토픽은 다시 만들지 않는다 (여러 번 띄워도 같은 결과)', async () => {
      mocks().listTopicsMock.mockResolvedValue([...ALL_TOPICS]);
      const provisioner = new OrderTopicProvisioner(buildConfig());

      const report = await provisioner.provision();

      expect(mocks().createTopicsMock).not.toHaveBeenCalled();
      expect(report.created).toEqual([]);
      expect(report.existing).toEqual(ALL_TOPICS);
    });

    it('일부만 없으면 없는 것만 만든다', async () => {
      mocks().listTopicsMock.mockResolvedValue([
        ORDER_TOPICS['orders.created'],
      ]);
      const provisioner = new OrderTopicProvisioner(buildConfig());

      const report = await provisioner.provision();

      expect(report.created).not.toContain('orders.created');
      expect(report.created).toHaveLength(ALL_TOPICS.length - 1);
      expect(report.existing).toEqual(['orders.created']);
    });

    it('작업 후 admin 연결을 끊는다', async () => {
      mocks().listTopicsMock.mockResolvedValue([]);
      await new OrderTopicProvisioner(buildConfig()).provision();

      expect(mocks().disconnectMock).toHaveBeenCalled();
    });

    it('중간에 실패해도 admin 연결을 끊는다', async () => {
      mocks().listTopicsMock.mockRejectedValue(new Error('브로커 응답 없음'));
      const provisioner = new OrderTopicProvisioner(buildConfig());

      await expect(provisioner.provision()).rejects.toThrow(/브로커 응답 없음/);
      expect(mocks().disconnectMock).toHaveBeenCalled();
    });
  });

  describe('파티션 부족 감지', () => {
    // 자동 생성에 맡기면 파티션 1개짜리가 조용히 생긴다.
    // 발행은 정상으로 보여서 실험 결과를 잘못 읽게 된다. 이 감지가 그걸 막는다.
    it('파티션이 모자란 기존 토픽을 찾아낸다', async () => {
      mocks().listTopicsMock.mockResolvedValue([...ALL_TOPICS]);
      stubMetadata({ 'orders.created': 1 });

      const report = await new OrderTopicProvisioner(
        buildConfig({ partitions: 6 }),
      ).provision();

      expect(report.underPartitioned).toEqual([
        { topic: 'orders.created', actual: 1, expected: 6 },
      ]);
    });

    it('파티션이 충분하면 보고하지 않는다', async () => {
      mocks().listTopicsMock.mockResolvedValue([...ALL_TOPICS]);
      stubMetadata({}, 6);

      const report = await new OrderTopicProvisioner(
        buildConfig({ partitions: 6 }),
      ).provision();

      expect(report.underPartitioned).toEqual([]);
    });

    it('기대보다 많은 건 문제로 보지 않는다', async () => {
      mocks().listTopicsMock.mockResolvedValue([...ALL_TOPICS]);
      stubMetadata({}, 12);

      const report = await new OrderTopicProvisioner(
        buildConfig({ partitions: 6 }),
      ).provision();

      expect(report.underPartitioned).toEqual([]);
    });

    // 파티션을 늘리면 같은 키가 다른 파티션으로 가서 기존 순서 보장이 깨진다.
    // 자동으로 늘리지 않고 사람에게 알린다.
    it('파티션이 모자라도 자동으로 늘리지 않는다', async () => {
      mocks().listTopicsMock.mockResolvedValue([...ALL_TOPICS]);
      stubMetadata({ 'orders.created': 1 });

      await new OrderTopicProvisioner(
        buildConfig({ partitions: 6 }),
      ).provision();

      expect(mocks().createTopicsMock).not.toHaveBeenCalled();
    });

    it('새로 만든 토픽은 검사 대상이 아니다', async () => {
      mocks().listTopicsMock.mockResolvedValue([]);

      const report = await new OrderTopicProvisioner(buildConfig()).provision();

      expect(mocks().fetchTopicMetadataMock).not.toHaveBeenCalled();
      expect(report.underPartitioned).toEqual([]);
    });
  });
});
