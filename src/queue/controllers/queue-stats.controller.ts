import { Controller, Get } from '@nestjs/common';
import { QueueService } from '../queue.service';

@Controller('queue-stats')
export class QueueStatsController {
  constructor(private readonly queueService: QueueService) {}

  @Get()
  getStats() {
    return {
      timestamp: new Date().toISOString(),
      ...this.queueService.getQueueStats(),
      memory: {
        heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
        heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        rss: Math.round(process.memoryUsage().rss / 1024 / 1024),
        external: Math.round(process.memoryUsage().external / 1024 / 1024),
      },
      uptime: process.uptime(),
    };
  }
}
