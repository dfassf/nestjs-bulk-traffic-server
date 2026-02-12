import { Injectable, Logger } from '@nestjs/common';
import { QueuePersistence } from './persistence.interface';
import { QueueSnapshot } from '../interfaces/queue-task.interface';
import * as fs from 'fs/promises';
import * as path from 'path';

@Injectable()
export class FilePersistenceService implements QueuePersistence {
  private readonly logger = new Logger(FilePersistenceService.name);
  private readonly snapshotPath: string;
  private readonly maxSnapshotAgeMs: number;

  constructor() {
    this.snapshotPath = path.resolve(
      process.env.QUEUE_SNAPSHOT_PATH || '.queue-snapshot.json',
    );
    this.maxSnapshotAgeMs = this.readPositiveIntEnv(
      'QUEUE_SNAPSHOT_MAX_AGE_MS',
      5 * 60 * 1000,
    );
  }

  async saveSnapshot(snapshot: QueueSnapshot): Promise<void> {
    try {
      await fs.writeFile(this.snapshotPath, JSON.stringify(snapshot), 'utf-8');
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`큐 스냅샷 저장 실패: ${message}`);
    }
  }

  async loadSnapshot(): Promise<QueueSnapshot | null> {
    try {
      const raw = await fs.readFile(this.snapshotPath, 'utf-8');
      const snapshot = JSON.parse(raw) as QueueSnapshot;

      if (Date.now() - snapshot.timestamp > this.maxSnapshotAgeMs) {
        this.logger.warn(
          `큐 스냅샷 만료(${this.maxSnapshotAgeMs}ms 초과)로 복구하지 않습니다.`,
        );
        await this.clearSnapshot();
        return null;
      }

      return snapshot;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return null;
      }

      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`큐 스냅샷 로드 실패: ${message}`);
      return null;
    }
  }

  async clearSnapshot(): Promise<void> {
    try {
      await fs.unlink(this.snapshotPath);
    } catch {
      // file does not exist
    }
  }

  private readPositiveIntEnv(name: string, fallback: number): number {
    const raw = process.env[name];
    if (!raw) return fallback;

    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return parsed;
  }
}
