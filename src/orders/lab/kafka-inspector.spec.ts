import { KafkaInspector } from './kafka-inspector';

/**
 * admin 클라이언트를 가짜로 세워 무엇을 호출하는지 본다.
 *
 * 되감기는 "무엇을 불렀는가" 가 곧 동작이라, 호출 인자를 확인하는 것이
 * 실제 결과를 확인하는 것과 같다.
 */
function createAdminStub(
  topicOffsets: { partition: number; low: string; high: string }[],
) {
  const calls: {
    setOffsets: { groupId: string; topic: string; partitions: any[] }[];
    resetOffsets: unknown[];
    connected: boolean;
    disconnected: boolean;
  } = {
    setOffsets: [],
    resetOffsets: [],
    connected: false,
    disconnected: false,
  };

  const admin = {
    connect: async () => {
      calls.connected = true;
    },
    disconnect: async () => {
      calls.disconnected = true;
    },
    fetchTopicOffsets: async () => topicOffsets,
    setOffsets: async (args: any) => {
      calls.setOffsets.push(args);
    },
    resetOffsets: async (args: any) => {
      calls.resetOffsets.push(args);
    },
  };

  return { admin, calls };
}

function buildInspector(admin: any): KafkaInspector {
  const inspector = new KafkaInspector({
    brokers: ['localhost:9092'],
    clientId: 'test',
  });

  // withAdmin 이 만드는 실제 연결 대신 가짜를 쓰게 한다.
  (inspector as any).withAdmin = async (work: (a: any) => Promise<unknown>) =>
    work(admin);

  return inspector;
}

describe('KafkaInspector.resetOffsets', () => {
  const topicOffsets = [
    { partition: 0, low: '10', high: '100' },
    { partition: 1, low: '0', high: '250' },
  ];

  /**
   * kafkajs 의 resetOffsets 는 커밋 기록을 지운다. 지워지면 컨슈머가
   * 자기 설정(fromBeginning)을 따르는데, 그게 false 면 최신부터 읽어서
   * 과거를 하나도 다시 안 읽는다. 되감기는 성공했다는데 재처리는 안 되는,
   * 조용히 틀리는 상태가 된다.
   */
  it('커밋을 지우지 않고 위치를 명시적으로 넣는다', async () => {
    const { admin, calls } = createAdminStub(topicOffsets);

    await buildInspector(admin).resetOffsets(
      'g1',
      'orders.created',
      'earliest',
    );

    expect(calls.setOffsets).toHaveLength(1);
    // 지우는 쪽은 부르면 안 된다.
    expect(calls.resetOffsets).toHaveLength(0);
  });

  it('earliest 는 각 파티션의 남아 있는 가장 오래된 위치로 보낸다', async () => {
    const { admin, calls } = createAdminStub(topicOffsets);

    await buildInspector(admin).resetOffsets(
      'g1',
      'orders.created',
      'earliest',
    );

    expect(calls.setOffsets[0].groupId).toBe('g1');
    expect(calls.setOffsets[0].topic).toBe('orders.created');
    // retention 으로 지워진 구간이 있으면 0 이 아니라 low 부터다.
    expect(calls.setOffsets[0].partitions).toEqual([
      { partition: 0, offset: '10' },
      { partition: 1, offset: '0' },
    ]);
  });

  it('latest 는 각 파티션의 최신 위치로 보낸다', async () => {
    const { admin, calls } = createAdminStub(topicOffsets);

    await buildInspector(admin).resetOffsets('g1', 'orders.created', 'latest');

    expect(calls.setOffsets[0].partitions).toEqual([
      { partition: 0, offset: '100' },
      { partition: 1, offset: '250' },
    ]);
  });

  it('어디로 보냈는지 돌려준다', async () => {
    const { admin } = createAdminStub(topicOffsets);

    const result = await buildInspector(admin).resetOffsets(
      'g1',
      'orders.created',
      'earliest',
    );

    expect(result.target).toBe('earliest');
    // 되감은 결과를 눈으로 확인할 수 있어야 한다.
    expect(result.offsets).toEqual([
      { partition: 0, offset: '10' },
      { partition: 1, offset: '0' },
    ]);
  });

  // 그룹에 컨슈머가 살아 있으면 카프카가 거부한다.
  // 그 사실을 조용히 삼키면 되감긴 줄 알고 실험을 이어가게 된다.
  it('카프카가 거부하면 이유와 함께 알린다', async () => {
    const { admin } = createAdminStub(topicOffsets);
    admin.setOffsets = async () => {
      throw new Error('The group is not empty');
    };

    await expect(
      buildInspector(admin).resetOffsets('g1', 'orders.created', 'earliest'),
    ).rejects.toThrow(/컨슈머가 모두 멈춰 있어야/);
  });
});
