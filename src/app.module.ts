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
import { OrderModule } from './orders/order.module';
import { validateEnv } from './config/env.validation';
import { QueueMiddleware } from './queue/middleware/queue.middleware';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, validate: validateEnv }),
    QueueModule,
    OrderModule,
  ],
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
        { path: 'benchmark-stats', method: RequestMethod.ALL },
        { path: 'go-engine-stats', method: RequestMethod.ALL },
        { path: 'load-test/(.*)', method: RequestMethod.ALL },
        // 주문 API 는 카프카 관찰용이다. 앞단 큐를 거치면 지연이 큐 때문인지
        // 카프카 때문인지 구분이 안 돼서 실험 결과를 못 읽는다.
        { path: 'orders', method: RequestMethod.ALL },
        { path: 'orders/(.*)', method: RequestMethod.ALL },
        // 실험 조작판. 부하를 거는 도중에도 조작이 막히면 안 된다.
        { path: 'lab/(.*)', method: RequestMethod.ALL },
      )
      .forRoutes({ path: '*', method: RequestMethod.ALL });
  }
}
