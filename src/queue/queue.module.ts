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
import { SqliteBenchService } from './sqlite-bench.service';
import { QueueSnapshotManager } from './queue-snapshot.manager';
import { QueueStateHolder } from './queue-state.holder';
import { QueueProcessorService } from './queue-processor.service';
import { WorkerTaskRouterService } from './worker-task-router.service';

@Module({
  imports: [PersistenceModule],
  controllers: [QueueStatsController, LoadTestController],
  providers: [
    QueueService,
    QueueStateHolder,
    QueueProcessorService,
    MemoryService,
    BatchService,
    WorkerPoolService,
    QueueOptionsParser,
    QueueStatsService,
    QueueSnapshotManager,
    WorkerHealthService,
    WorkerTaskRouterService,
    GoEngineClient,
    EngineRouterService,
    SqliteBenchService,
  ],
  exports: [QueueService, MemoryService, WorkerPoolService, EngineRouterService],
})
export class QueueModule {}
