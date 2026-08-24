import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Post,
  Query,
} from '@nestjs/common';
import {
  ConsumerProcessManager,
  SpawnConsumerOptions,
} from './consumer-process.manager';
import { KafkaInspector } from './kafka-inspector';
import { toErrorMessage } from '../../queue/utils/error-message';

const DEFAULT_GROUP = 'order-processor';

/**
 * 카프카 실험 조작판 API.
 *
 * 컨슈머를 띄우고 죽이고, Lag 을 보고, 오프셋을 되감는다.
 * 실험 목록은 docs/kafka-lab-plan.md 참고.
 */
@Controller('lab')
export class LabController {
  constructor(
    private readonly processes: ConsumerProcessManager,
    private readonly inspector: KafkaInspector,
  ) {}

  @Post('consumers')
  spawn(@Body() body: SpawnConsumerOptions) {
    try {
      const info = this.processes.spawnConsumer(body ?? {});
      return {
        pid: info.pid,
        groupId: info.groupId,
        instances: info.instances,
        options: info.options,
      };
    } catch (error) {
      // 잘못된 설정값은 사용자 실수다. 500 이 아니라 400 으로 알린다.
      throw new BadRequestException(toErrorMessage(error));
    }
  }

  @Get('consumers')
  list() {
    return { processes: this.processes.list() };
  }

  /**
   * 컨슈머를 종료한다.
   *
   * signal=SIGKILL(기본): 커밋할 틈 없이 즉시 죽는다. 재시작하면 중복이 관측된다.
   * signal=SIGTERM: 정상 종료. 커밋하고 빠지므로 중복이 안 생긴다.
   */
  @Delete('consumers')
  stop(@Query('pid') pid?: string, @Query('signal') signal?: string) {
    const resolvedSignal = (signal ?? 'SIGKILL') as NodeJS.Signals;
    if (resolvedSignal !== 'SIGKILL' && resolvedSignal !== 'SIGTERM') {
      throw new BadRequestException(
        'signal 은 SIGKILL 또는 SIGTERM 이어야 합니다.',
      );
    }

    if (!pid) {
      const stopped = this.processes.stopAll(resolvedSignal);
      return { stopped, signal: resolvedSignal };
    }

    const parsed = Number(pid);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new BadRequestException(`pid 가 올바르지 않습니다: ${pid}`);
    }

    try {
      const info = this.processes.stop(parsed, resolvedSignal);
      return { pid: info.pid, signal: resolvedSignal };
    } catch (error) {
      throw new BadRequestException(toErrorMessage(error));
    }
  }

  @Delete('consumers/finished')
  clearFinished() {
    return { cleared: this.processes.clearFinished() };
  }

  @Get('topics')
  async topics() {
    return this.inspector.describeTopics();
  }

  @Get('groups')
  async groups() {
    return { groups: await this.inspector.listGroups() };
  }

  @Get('lag')
  async lag(@Query('groupId') groupId?: string) {
    try {
      return await this.inspector.describeGroup(groupId || DEFAULT_GROUP);
    } catch (error) {
      throw new BadRequestException(toErrorMessage(error));
    }
  }

  @Post('offsets/reset')
  async resetOffsets(
    @Body()
    body: {
      groupId?: string;
      topic?: string;
      target?: 'earliest' | 'latest';
    },
  ) {
    const topic = body?.topic;
    if (!topic) {
      throw new BadRequestException('되감을 토픽을 지정해야 합니다.');
    }

    const target = body?.target ?? 'earliest';
    if (target !== 'earliest' && target !== 'latest') {
      throw new BadRequestException(
        'target 은 earliest 또는 latest 여야 합니다.',
      );
    }

    try {
      return await this.inspector.resetOffsets(
        body?.groupId || DEFAULT_GROUP,
        topic,
        target,
      );
    } catch (error) {
      throw new BadRequestException(toErrorMessage(error));
    }
  }
}
