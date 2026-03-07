import { Body, Controller, Delete, Get, HttpCode, Post, Sse } from '@nestjs/common';
import { Observable } from 'rxjs';
import { EngineRouterService } from '../engine-router.service';
import { LoadTestCompareService } from '../load-test/load-test-compare.service';
import { LoadTestRunnerService } from '../load-test/load-test-runner.service';

@Controller('load-test')
export class LoadTestController {
  constructor(
    private readonly runner: LoadTestRunnerService,
    private readonly compare: LoadTestCompareService,
    private readonly engineRouter: EngineRouterService,
  ) {}

  @Post('cpu')
  @HttpCode(200)
  async cpuTask(@Body() body: { iterations?: number; priority?: number }) {
    return this.runner.enqueueCpuTask(body);
  }

  @Post('io')
  @HttpCode(200)
  async ioTask(@Body() body: { delayMs?: number; priority?: number }) {
    return this.runner.enqueueIoTask(body);
  }

  @Post('batch')
  @HttpCode(200)
  async batchTask(@Body() body: { itemCount?: number; priority?: number }) {
    return this.runner.enqueueBatchTask(body);
  }

  @Post('mixed')
  @HttpCode(200)
  async mixedBurst(
    @Body() body: { count?: number; cpuRatio?: number; ioRatio?: number },
  ) {
    return this.runner.runMixedBurst(body);
  }

  @Post('compare')
  @HttpCode(200)
  async compareEngines(@Body() body: { count?: number; max?: number }) {
    return this.compare.compareEngines(body);
  }

  @Post('db-write')
  @HttpCode(200)
  dbWrite(@Body() body: { count?: number }) {
    return this.runner.dbWrite(body);
  }

  @Post('db-read')
  @HttpCode(200)
  dbRead(@Body() body: { count?: number }) {
    return this.runner.dbRead(body);
  }

  @Get('db-rows')
  async dbRows() {
    return this.runner.dbRows();
  }

  @Delete('db-reset')
  async dbReset() {
    return this.runner.dbReset();
  }

  @Get('ping')
  ping() {
    return {
      ok: true,
      engine: this.engineRouter.getEngine(),
      dbDriver: process.env.BENCH_DB_DRIVER || 'sqlite',
      timestamp: Date.now(),
    };
  }

  @Post('run-stream')
  @HttpCode(200)
  @Sse()
  runStream(
    @Body() body: {
      type: 'cpu' | 'io' | 'mixed' | 'db-write' | 'db-read';
      count?: number;
      iterations?: number;
      delayMs?: number;
      cpuRatio?: number;
      ioRatio?: number;
      max?: number;
    },
  ): Observable<MessageEvent> {
    return this.runner.runStream(body);
  }

  @Post('compare-stream')
  @HttpCode(200)
  @Sse()
  compareStream(
    @Body() body: {
      count?: number;
      max?: number;
      testType?: 'cpu' | 'io';
      delayMs?: number;
    },
  ): Observable<MessageEvent> {
    return this.compare.compareStream(body);
  }
}
