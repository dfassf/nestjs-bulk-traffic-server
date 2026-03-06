import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as os from 'os';
import * as path from 'path';
import { AllExceptionsFilter } from './filters/all-exceptions.filter';

const logger = new Logger('Bootstrap');

process.on('uncaughtException', (error) => {
  logger.error(`처리되지 않은 예외 발생: ${error.message}`);
  logger.error(error.stack);

  const isFatalError =
    error.name === 'ReferenceError' ||
    error.name === 'TypeError' ||
    error.message.includes('ENOTFOUND') ||
    error.message.includes('ECONNREFUSED') ||
    error.message.includes('EADDRINUSE');

  if (isFatalError) {
    logger.error('복구 불가능한 에러로 인해 프로세스를 종료합니다.');
    setTimeout(() => process.exit(1), 1000);
  } else {
    logger.warn('에러가 발생했지만 서비스는 계속 실행됩니다.');
  }
});

process.on('unhandledRejection', (reason) => {
  logger.error(`처리되지 않은 Promise 거부: ${reason}`);

  if (reason instanceof Error) {
    const isFatalError =
      reason.name === 'ReferenceError' ||
      reason.name === 'TypeError' ||
      reason.message.includes('ENOTFOUND') ||
      reason.message.includes('ECONNREFUSED');

    if (isFatalError) {
      logger.error('복구 불가능한 Promise 거부로 인해 프로세스를 종료합니다.');
      setTimeout(() => process.exit(1), 1000);
    }
  }
});

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: ['error', 'warn', 'log'],
    bodyParser: true,
  });

  app.useStaticAssets(path.resolve(process.cwd(), 'public'));

  const configService = app.get(ConfigService);

  const threadPoolSize = configService.get<string>(
    'UV_THREADPOOL_SIZE',
    '64',
  );
  process.env.UV_THREADPOOL_SIZE = threadPoolSize;
  logger.log(`libuv 스레드풀 크기 설정: ${threadPoolSize}`);

  const allowedOrigins = configService
    .get<string>('ALLOWED_ORIGINS', 'http://localhost:3000')
    .split(',');

  app.enableCors({
    origin: allowedOrigins,
    credentials: true,
  });

  const memoryUsageLogger = setInterval(() => {
    const memoryUsage = process.memoryUsage();
    const usageRatio = memoryUsage.heapUsed / memoryUsage.heapTotal;

    logger.debug(
      `메모리 사용량: ${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB / ${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`,
    );

    if (usageRatio > 0.85) {
      logger.warn(
        `높은 메모리 사용량 감지: ${Math.round(usageRatio * 100)}%`,
      );
    }
  }, 30000);

  app.useGlobalFilters(new AllExceptionsFilter());

  app.use((req, res, next) => {
    const startTime = Date.now();
    res.on('finish', () => {
      const duration = Date.now() - startTime;
      if (duration > 1000) {
        logger.warn(
          `[성능 경고] ${req.method} ${req.url} - ${duration}ms 소요`,
        );
      }
    });
    next();
  });

  app.use('/health', (req, res) => {
    const memoryUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memoryUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memoryUsage.heapTotal / 1024 / 1024);
    const usageRatio = memoryUsage.heapUsed / memoryUsage.heapTotal;

    res.status(200).json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      memory: {
        used: `${heapUsedMB}MB`,
        total: `${heapTotalMB}MB`,
        percent: `${Math.round(usageRatio * 100)}%`,
        rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`,
      },
      system: {
        arch: process.arch,
        platform: process.platform,
        cpus: os.cpus().length,
        loadAvg: os.loadavg(),
        freeMemory: `${Math.round(os.freemem() / 1024 / 1024)}MB`,
        totalMemory: `${Math.round(os.totalmem() / 1024 / 1024)}MB`,
      },
      processId: process.pid,
      nodeVersion: process.version,
      workersEnabled: process.env.DISABLE_WORKERS !== 'true',
    });
  });

  const port = configService.get<number>('PORT') ?? 3000;
  await app.listen(port);
  logger.log(`서버 시작됨 - 포트: ${port}, 프로세스 ID: ${process.pid}`);

  process.on('SIGTERM', async () => {
    logger.log('SIGTERM 신호 수신. 서버 정상 종료 중...');
    clearInterval(memoryUsageLogger);
    await app.close();
    process.exit(0);
  });
}

bootstrap();
