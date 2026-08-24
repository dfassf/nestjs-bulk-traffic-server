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
import { GoEngineBackend } from './go-engine.backend';
import { EngineRouterService } from './engine-router.service';
import {
  KafkaProducerBackend,
  KAFKA_PRODUCER_CONFIG,
  kafkaProducerConfigFromEnv,
} from './kafka-producer.backend';
import { readWorkerEngineEnv } from './utils/env';
import { BENCH_DRIVER } from './bench-driver.interface';
import { SqliteBenchDriver } from './sqlite-bench.driver';
import { PgBenchDriver } from './pg-bench.driver';
import { QueueSnapshotManager } from './queue-snapshot.manager';
import { QueueStateHolder } from './queue-state.holder';
import { QueueProcessorService } from './queue-processor.service';
import { SimulationService } from './simulation.service';
import { WorkerTaskRouterService } from './worker-task-router.service';
import { LoadTestRunnerService } from './load-test/load-test-runner.service';
import { LoadTestCompareService } from './load-test/load-test-compare.service';

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
    SimulationService,
    GoEngineClient,
    GoEngineBackend,
    KafkaProducerBackend,
    EngineRouterService,
    LoadTestRunnerService,
    LoadTestCompareService,
    {
      // 프로듀서가 스스로 켜짐 여부를 판단하지 않도록 모듈이 정해서 넘긴다.
      provide: KAFKA_PRODUCER_CONFIG,
      useFactory: () =>
        kafkaProducerConfigFromEnv(readWorkerEngineEnv() === 'kafka'),
    },
    {
      provide: BENCH_DRIVER,
      useFactory: async () => {
        const driver =
          process.env.BENCH_DB_DRIVER === 'postgresql'
            ? new PgBenchDriver()
            : new SqliteBenchDriver();
        await driver.init();
        return driver;
      },
    },
  ],
  exports: [
    QueueService,
    MemoryService,
    WorkerPoolService,
    EngineRouterService,
  ],
})
export class QueueModule {}
