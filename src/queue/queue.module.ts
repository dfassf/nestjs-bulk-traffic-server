import { Module } from '@nestjs/common';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { QueueStatsController } from './controllers/queue-stats.controller';
import { WorkerPoolService } from './worker-pool.service';
import { PersistenceModule } from './persistence/persistence.module';

@Module({
  imports: [PersistenceModule],
  controllers: [QueueStatsController],
  providers: [QueueService, MemoryService, BatchService, WorkerPoolService],
  exports: [QueueService, MemoryService, WorkerPoolService],
})
export class QueueModule {}
