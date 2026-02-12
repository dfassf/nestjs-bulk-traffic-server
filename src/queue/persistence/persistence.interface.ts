import { QueueSnapshot } from '../interfaces/queue-task.interface';

export const QUEUE_PERSISTENCE = 'QUEUE_PERSISTENCE';

export interface QueuePersistence {
  saveSnapshot(snapshot: QueueSnapshot): Promise<void>;
  loadSnapshot(): Promise<QueueSnapshot | null>;
  clearSnapshot(): Promise<void>;
}
