import { Module } from '@nestjs/common';
import { QueueService } from './queue.service';
import { MemoryService } from './memory.service';
import { BatchService } from './batch.service';
import { QueueStatsController } from './controllers/queue-stats.controller';
import { LoadTestController } from './controllers/load-test.controller';
import { WorkerPoolService } from './worker-pool.service';
import { QueueOptionsParser } from './queue-options.parser';
import { QueueStatsService } from './queue-stats.service';
import { WorkerHealthService } from './worker-health.service';
import { PersistenceModule } from './persistence/persistence.module';
import { GoEngineClient } from './go-engine.client';
import { EngineRouterService } from './engine-router.service';
import { BenchmarkService } from './benchmark.service';

@Module({
  imports: [PersistenceModule],
  controllers: [QueueStatsController, LoadTestController],
  providers: [
    QueueService,
    MemoryService,
    BatchService,
    WorkerPoolService,
    QueueOptionsParser,
    QueueStatsService,
    WorkerHealthService,
    GoEngineClient,
    EngineRouterService,
    BenchmarkService,
  ],
  exports: [QueueService, MemoryService, WorkerPoolService, EngineRouterService, BenchmarkService],
})
export class QueueModule {}
