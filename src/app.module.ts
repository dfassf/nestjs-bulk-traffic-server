import {
  MiddlewareConsumer,
  Module,
  NestModule,
  RequestMethod,
} from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { QueueModule } from './queue/queue.module';
import { validateEnv } from './config/env.validation';
import { QueueMiddleware } from './queue/middleware/queue.middleware';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true, validate: validateEnv }), QueueModule],
  controllers: [AppController],
  providers: [AppService, QueueMiddleware],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer
      .apply(QueueMiddleware)
      .exclude(
        { path: 'health', method: RequestMethod.ALL },
        { path: 'queue-stats', method: RequestMethod.ALL },
      )
      .forRoutes({ path: '*', method: RequestMethod.ALL });
  }
}
