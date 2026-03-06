import { Controller, Get } from '@nestjs/common';
import { QueueService } from '../queue.service';
import { EngineRouterService } from '../engine-router.service';
import { GoEngineClient } from '../go-engine.client';
import { toErrorMessage } from '../utils/error-message';

@Controller()
export class QueueStatsController {
  constructor(
    private readonly queueService: QueueService,
    private readonly engineRouter: EngineRouterService,
    private readonly goEngineClient: GoEngineClient,
  ) {}

  @Get('queue-stats')
  getStats() {
    return {
      timestamp: new Date().toISOString(),
      ...this.queueService.getQueueStats(),
      engineMode: this.engineRouter.getEngine(),
      goEngineConnected: this.goEngineClient.isConnected(),
      memory: {
        heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
        heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        rss: Math.round(process.memoryUsage().rss / 1024 / 1024),
        external: Math.round(process.memoryUsage().external / 1024 / 1024),
      },
      uptime: process.uptime(),
    };
  }

  @Get('go-engine-stats')
  async getGoEngineStats() {
    try {
      const stats = await this.goEngineClient.getStats();
      return { connected: true, ...stats };
    } catch (error) {
      return {
        connected: false,
        error: toErrorMessage(error),
      };
    }
  }
}
