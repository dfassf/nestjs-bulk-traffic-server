import { KafkaProducerBackend, pickTopic, KAFKA_TOPICS } from './kafka-producer.backend';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

jest.mock('kafkajs', () => {
  const sendMock = jest.fn();
  const connectMock = jest.fn();
  const disconnectMock = jest.fn();
  const producerCtor = jest.fn(() => ({
    connect: connectMock,
    disconnect: disconnectMock,
    send: sendMock,
  }));
  const kafkaCtor = jest.fn(() => ({
    producer: producerCtor,
    admin: jest.fn(() => ({
      connect: jest.fn(),
      listTopics: jest.fn().mockResolvedValue(['tasks.high', 'tasks.normal', 'tasks.low']),
      disconnect: jest.fn(),
    })),
  }));
  return {
    Kafka: kafkaCtor,
    logLevel: { WARN: 2 },
    __mocks: { sendMock, connectMock, disconnectMock, producerCtor, kafkaCtor },
  };
});

const kafkaMocks = () => (jest.requireMock('kafkajs') as any).__mocks;

function buildTask(overrides: Partial<QueueTask> = {}): QueueTask {
  return {
    id: 42,
    requestId: 'req-42',
    execute: async () => null,
    resolve: () => {},
    reject: () => {},
    timestamp: 1_700_000_000_000,
    priority: 0,
    workloadType: WorkloadType.CPU,
    category: 'default',
    params: { hello: 'world' },
    ...overrides,
  };
}

describe('pickTopic', () => {
  it('priority >= 5 -> high', () => {
    expect(pickTopic(5)).toBe(KAFKA_TOPICS.HIGH);
    expect(pickTopic(10)).toBe(KAFKA_TOPICS.HIGH);
  });

  it('0 <= priority < 5 -> normal', () => {
    expect(pickTopic(0)).toBe(KAFKA_TOPICS.NORMAL);
    expect(pickTopic(4)).toBe(KAFKA_TOPICS.NORMAL);
  });

  it('priority < 0 -> low', () => {
    expect(pickTopic(-1)).toBe(KAFKA_TOPICS.LOW);
    expect(pickTopic(-100)).toBe(KAFKA_TOPICS.LOW);
  });
});

describe('KafkaProducerBackend', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    process.env.WORKER_ENGINE = 'kafka';
    process.env.KAFKA_BROKERS = 'broker-a:9092,broker-b:9092';
    process.env.KAFKA_CLIENT_ID = 'test-client';
  });

  afterEach(() => {
    delete process.env.WORKER_ENGINE;
    delete process.env.KAFKA_BROKERS;
    delete process.env.KAFKA_CLIENT_ID;
  });

  it('WORKER_ENGINE != kafka 이면 프로듀서 연결을 시도하지 않는다', async () => {
    process.env.WORKER_ENGINE = 'node';
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();
    expect(kafkaMocks().kafkaCtor).not.toHaveBeenCalled();
  });

  it('onModuleInit 시 브로커 목록과 clientId로 카프카 클라이언트 생성', async () => {
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();
    expect(kafkaMocks().kafkaCtor).toHaveBeenCalledWith(
      expect.objectContaining({
        clientId: 'test-client',
        brokers: ['broker-a:9092', 'broker-b:9092'],
      }),
    );
    expect(kafkaMocks().connectMock).toHaveBeenCalled();
    expect(backend.isConnected()).toBe(true);
  });

  it('priority >= 5 인 작업은 tasks.high 로 발행된다', async () => {
    kafkaMocks().sendMock.mockResolvedValue([
      { topicName: 'tasks.high', partition: 0, baseOffset: '10', errorCode: 0 },
    ]);
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    const task = buildTask({ priority: 7 });
    const result = await backend.execute(task);

    expect(kafkaMocks().sendMock).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: 'tasks.high',
        messages: [expect.objectContaining({ key: 'req-42' })],
      }),
    );
    if (result.mode === 'async') {
      expect(result.dispatch.topic).toBe('tasks.high');
      expect(result.dispatch.partition).toBe(0);
      expect(result.dispatch.offset).toBe('10');
      expect(result.backend).toBe('kafka');
    } else {
      fail('kafka backend는 async 모드여야 함');
    }
  });

  it('priority 0~4 는 tasks.normal 로 발행된다', async () => {
    kafkaMocks().sendMock.mockResolvedValue([
      { topicName: 'tasks.normal', partition: 1, baseOffset: '3', errorCode: 0 },
    ]);
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    await backend.execute(buildTask({ priority: 0 }));
    expect(kafkaMocks().sendMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ topic: 'tasks.normal' }),
    );

    await backend.execute(buildTask({ priority: 4 }));
    expect(kafkaMocks().sendMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ topic: 'tasks.normal' }),
    );
  });

  it('priority < 0 은 tasks.low 로 발행된다', async () => {
    kafkaMocks().sendMock.mockResolvedValue([
      { topicName: 'tasks.low', partition: 2, baseOffset: '0', errorCode: 0 },
    ]);
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    await backend.execute(buildTask({ priority: -1 }));
    expect(kafkaMocks().sendMock).toHaveBeenLastCalledWith(
      expect.objectContaining({ topic: 'tasks.low' }),
    );
  });

  it('메시지 페이로드에 taskId·priority·params 가 실려 나간다', async () => {
    kafkaMocks().sendMock.mockResolvedValue([
      { topicName: 'tasks.normal', partition: 0, baseOffset: '0', errorCode: 0 },
    ]);
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    await backend.execute(buildTask({ priority: 3, params: { userId: 'abc' } }));

    const call = kafkaMocks().sendMock.mock.calls[0][0];
    const value = JSON.parse(call.messages[0].value);
    expect(value.taskId).toBe(42);
    expect(value.priority).toBe(3);
    expect(value.params).toEqual({ userId: 'abc' });
  });

  it('key 는 requestId 우선, 없으면 taskId 로 매긴다', async () => {
    kafkaMocks().sendMock.mockResolvedValue([
      { topicName: 'tasks.normal', partition: 0, baseOffset: '0', errorCode: 0 },
    ]);
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();

    await backend.execute(buildTask({ requestId: 'req-xyz' }));
    expect(kafkaMocks().sendMock.mock.calls[0][0].messages[0].key).toBe('req-xyz');

    await backend.execute(buildTask({ requestId: undefined, id: 99 }));
    expect(kafkaMocks().sendMock.mock.calls[1][0].messages[0].key).toBe('99');
  });

  it('연결되지 않은 상태에서 execute 호출 시 오류', async () => {
    process.env.WORKER_ENGINE = 'node';
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();
    await expect(backend.execute(buildTask())).rejects.toThrow(/연결되지 않았습니다/);
  });

  it('onModuleDestroy 시 프로듀서 연결 해제', async () => {
    const backend = new KafkaProducerBackend();
    await backend.onModuleInit();
    await backend.onModuleDestroy();
    expect(kafkaMocks().disconnectMock).toHaveBeenCalled();
    expect(backend.isConnected()).toBe(false);
  });
});
