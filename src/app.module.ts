import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { QueueModule } from './queue/queue.module';
import { validateEnv } from './config/env.validation';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true, validate: validateEnv }), QueueModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
