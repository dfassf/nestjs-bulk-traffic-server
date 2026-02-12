import { EventEmitter } from 'events';
import { NextFunction, Request, Response } from 'express';
import { QueueService } from '../queue.service';
import { QueueMiddleware } from './queue.middleware';

type QueueServiceMock = Pick<QueueService, 'enqueue' | 'getQueueStats'> & {
  enqueue: jest.Mock;
  getQueueStats: jest.Mock;
};

type MockResponse = Response &
  EventEmitter & {
    status: jest.Mock;
    send: jest.Mock;
    headersSent: boolean;
  };

const createResponse = (): MockResponse => {
  const res = new EventEmitter() as MockResponse;
  res.headersSent = false;
  res.status = jest.fn().mockReturnValue(res);
  res.send = jest.fn().mockImplementation(() => {
    res.headersSent = true;
    setImmediate(() => res.emit('finish'));
    return res;
  });
  return res;
};

const createRequest = (overrides: Partial<Request> = {}): Request =>
  ({
    method: 'GET',
    path: '/api/users',
    url: '/api/users',
    query: {},
    body: {},
    ...overrides,
  }) as Request;

describe('QueueMiddleware', () => {
  let middleware: QueueMiddleware;
  let queueService: QueueServiceMock;

  beforeEach(() => {
    queueService = {
      enqueue: jest.fn(),
      getQueueStats: jest.fn().mockReturnValue({ memoryPressure: false }),
    } as QueueServiceMock;

    middleware = new QueueMiddleware(queueService as unknown as QueueService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('헬스체크 경로는 큐를 우회해야 한다', async () => {
    const req = createRequest({ path: '/health', url: '/health' });
    const res = createResponse();
    const next: NextFunction = jest.fn();

    await middleware.use(req, res, next);

    expect(next).toHaveBeenCalledTimes(1);
    expect(queueService.enqueue).not.toHaveBeenCalled();
  });

  it('일반 요청은 큐에 등록 후 next를 호출해야 한다', async () => {
    const req = createRequest({ path: '/api/user/profile', url: '/api/user/profile' });
    const res = createResponse();
    const next: NextFunction = jest.fn(() => {
      setImmediate(() => res.emit('finish'));
    });

    queueService.enqueue.mockImplementation(
      async (execute: () => Promise<unknown>) => execute(),
    );

    await middleware.use(req, res, next);

    expect(queueService.enqueue).toHaveBeenCalledTimes(1);
    const [, options] = queueService.enqueue.mock.calls[0];
    expect(options).toMatchObject({
      category: 'user',
      batch: true,
    });
    expect(next).toHaveBeenCalledTimes(1);
  });

  it('메모리 압박 상태에서 낮은 우선순위 요청은 즉시 거부해야 한다', async () => {
    queueService.getQueueStats.mockReturnValue({ memoryPressure: true });

    const req = createRequest({
      method: 'POST',
      path: '/api/bulk/import',
      url: '/api/bulk/import',
    });
    const res = createResponse();
    const next: NextFunction = jest.fn();

    await middleware.use(req, res, next);

    expect(queueService.enqueue).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(503);
    expect(next).not.toHaveBeenCalled();
  });

  it('요청 body의 workloadType과 functionCode를 큐 옵션으로 전달해야 한다', async () => {
    const req = createRequest({
      method: 'POST',
      path: '/api/tasks/execute',
      url: '/api/tasks/execute',
      body: {
        workloadType: 'gpu',
        functionCode: 'return params.a + params.b;',
        params: { a: 1, b: 2 },
      },
    });
    const res = createResponse();
    const next: NextFunction = jest.fn(() => {
      setImmediate(() => res.emit('finish'));
    });

    queueService.enqueue.mockImplementation(
      async (execute: () => Promise<unknown>) => execute(),
    );

    await middleware.use(req, res, next);

    const [, options] = queueService.enqueue.mock.calls[0];
    expect(options.workloadType).toBe('custom');
    expect(options.functionCode).toBe('return params.a + params.b;');
    expect(options.params).toEqual({ a: 1, b: 2 });
  });
});
