import { Module } from '@nestjs/common';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { QueueStatsController } from './controllers/queue-stats.controller';

@Module({
  controllers: [QueueStatsController],
  providers: [QueueService, MemoryService, BatchService],
  exports: [QueueService, MemoryService],
})
export class QueueModule {}
