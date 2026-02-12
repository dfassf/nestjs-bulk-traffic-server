import * as fs from 'fs/promises';
import * as path from 'path';
import { QueueSnapshot } from '../interfaces/queue-task.interface';
import { FilePersistenceService } from './file-persistence.service';

describe('FilePersistenceService', () => {
  const originalSnapshotPath = process.env.QUEUE_SNAPSHOT_PATH;
  const originalSnapshotMaxAge = process.env.QUEUE_SNAPSHOT_MAX_AGE_MS;
  const snapshotPath = path.resolve(
    process.cwd(),
    '.queue-snapshot.file-persistence.spec.json',
  );

  const createSnapshot = (timestamp = Date.now()): QueueSnapshot => ({
    timestamp,
    queues: {
      high: [{ id: 1, timestamp, priority: 10 }],
      normal: [{ id: 2, timestamp, priority: 0 }],
      low: [{ id: 3, timestamp, priority: -1 }],
    },
    stats: {
      totalProcessed: 100,
      totalRejected: 5,
      totalTimeout: 2,
      taskIdCounter: 500,
    },
  });

  const cleanupSnapshot = async (): Promise<void> => {
    await fs.unlink(snapshotPath).catch(() => undefined);
  };

  beforeEach(async () => {
    process.env.QUEUE_SNAPSHOT_PATH = snapshotPath;
    process.env.QUEUE_SNAPSHOT_MAX_AGE_MS = '300000';
    await cleanupSnapshot();
  });

  afterEach(async () => {
    await cleanupSnapshot();
    process.env.QUEUE_SNAPSHOT_PATH = originalSnapshotPath;
    process.env.QUEUE_SNAPSHOT_MAX_AGE_MS = originalSnapshotMaxAge;
  });

  it('스냅샷을 저장하고 다시 로드할 수 있어야 한다', async () => {
    const service = new FilePersistenceService();
    const snapshot = createSnapshot();

    await service.saveSnapshot(snapshot);
    const loaded = await service.loadSnapshot();

    expect(loaded).toEqual(snapshot);
  });

  it('만료된 스냅샷은 null을 반환하고 파일을 삭제해야 한다', async () => {
    process.env.QUEUE_SNAPSHOT_MAX_AGE_MS = '1000';
    const service = new FilePersistenceService();
    const expiredSnapshot = createSnapshot(Date.now() - 5000);

    await service.saveSnapshot(expiredSnapshot);
    const loaded = await service.loadSnapshot();

    expect(loaded).toBeNull();
    await expect(fs.access(snapshotPath)).rejects.toBeDefined();
  });

  it('스냅샷 파일이 없으면 null을 반환해야 한다', async () => {
    const service = new FilePersistenceService();
    const loaded = await service.loadSnapshot();
    expect(loaded).toBeNull();
  });
});
