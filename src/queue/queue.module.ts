import { Module } from '@nestjs/common';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { QueueStatsController } from './controllers/queue-stats.controller';
import { WorkerPoolService } from './worker-pool.service';
import { QueueOptionsParser } from './queue-options.parser';
import { QueueStatsService } from './queue-stats.service';
import { WorkerHealthService } from './worker-health.service';
import { PersistenceModule } from './persistence/persistence.module';

@Module({
  imports: [PersistenceModule],
  controllers: [QueueStatsController],
  providers: [
    QueueService,
    MemoryService,
    BatchService,
    WorkerPoolService,
    QueueOptionsParser,
    QueueStatsService,
    WorkerHealthService,
  ],
  exports: [QueueService, MemoryService, WorkerPoolService],
})
export class QueueModule {}
