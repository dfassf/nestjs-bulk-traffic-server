import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { Admin, Kafka, logLevel } from 'kafkajs';
import { ORDER_TOPICS } from '../order-events';
import { readKafkaBrokersEnv } from '../../queue/utils/env';

export const KAFKA_INSPECTOR_CONFIG = Symbol('KAFKA_INSPECTOR_CONFIG');

export interface InspectorConfig {
  brokers: string[];
  clientId: string;
}

export function inspectorConfigFromEnv(): InspectorConfig {
  return {
    brokers: readKafkaBrokersEnv(),
    clientId: process.env.LAB_ADMIN_CLIENT_ID ?? 'kafka-lab-inspector',
  };
}

export interface PartitionLag {
  partition: number;
  /** 브로커에 쌓인 마지막 오프셋 */
  latestOffset: string;
  /** 이 그룹이 처리했다고 기록한 위치. 아직 한 번도 커밋 안 했으면 null */
  committedOffset: string | null;
  /** 아직 처리 못 한 건수. 커밋 기록이 없으면 전체가 밀린 것으로 본다 */
  lag: number;
}

export interface TopicLag {
  topic: string;
  totalLag: number;
  partitions: PartitionLag[];
}

export interface GroupMemberInfo {
  memberId: string;
  clientId: string;
  host: string;
}

export interface GroupInfo {
  groupId: string;
  state: string;
  members: GroupMemberInfo[];
  topics: TopicLag[];
}

/**
 * 카프카 상태를 들여다본다.
 *
 * Lag(밀린 건수), 파티션 분배, 그룹 상태를 조회하고 오프셋을 되감는다.
 * 실험 조작판이 보여줄 숫자를 여기서 만든다.
 */
@Injectable()
export class KafkaInspector {
  private readonly logger = new Logger(KafkaInspector.name);
  private readonly config: InspectorConfig;

  constructor(
    @Optional()
    @Inject(KAFKA_INSPECTOR_CONFIG)
    config?: InspectorConfig,
  ) {
    this.config = config ?? inspectorConfigFromEnv();
  }

  private async withAdmin<T>(work: (admin: Admin) => Promise<T>): Promise<T> {
    if (this.config.brokers.length === 0) {
      throw new Error(
        'Kafka 브로커 주소가 비어 있습니다. KAFKA_BROKERS 를 확인하세요.',
      );
    }

    const kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
      logLevel: logLevel.NOTHING,
    });
    const admin = kafka.admin();
    await admin.connect();
    try {
      return await work(admin);
    } finally {
      await admin.disconnect();
    }
  }

  /** 토픽별 파티션 수와 쌓인 메시지 수. */
  async describeTopics(topics: string[] = Object.values(ORDER_TOPICS)) {
    return this.withAdmin(async (admin) => {
      const existing = await admin.listTopics();
      const present = topics.filter((topic) => existing.includes(topic));

      const results = [];
      for (const topic of present) {
        const offsets = await admin.fetchTopicOffsets(topic);
        results.push({
          topic,
          partitions: offsets.length,
          messages: offsets.reduce(
            (sum, entry) =>
              sum + Math.max(Number(entry.high) - Number(entry.low), 0),
            0,
          ),
          perPartition: offsets
            .map((entry) => ({
              partition: entry.partition,
              count: Math.max(Number(entry.high) - Number(entry.low), 0),
            }))
            .sort((a, b) => a.partition - b.partition),
        });
      }

      return {
        topics: results,
        missing: topics.filter((topic) => !existing.includes(topic)),
      };
    });
  }

  /** 컨슈머 그룹 목록. */
  async listGroups(): Promise<{ groupId: string; protocolType: string }[]> {
    return this.withAdmin(async (admin) => {
      const { groups } = await admin.listGroups();
      return groups.map((group) => ({
        groupId: group.groupId,
        protocolType: group.protocolType,
      }));
    });
  }

  /** 그룹의 상태와 토픽별 Lag. 조작판의 핵심 지표다. */
  async describeGroup(
    groupId: string,
    topics: string[] = Object.values(ORDER_TOPICS),
  ): Promise<GroupInfo> {
    return this.withAdmin(async (admin) => {
      const description = await admin.describeGroups([groupId]);
      const group = description.groups[0];

      const existing = await admin.listTopics();
      const present = topics.filter((topic) => existing.includes(topic));

      const topicLags: TopicLag[] = [];
      for (const topic of present) {
        topicLags.push(await this.topicLag(admin, groupId, topic));
      }

      return {
        groupId,
        state: group?.state ?? 'Unknown',
        members: (group?.members ?? []).map((member) => ({
          memberId: member.memberId,
          clientId: member.clientId,
          host: member.clientHost,
        })),
        topics: topicLags,
      };
    });
  }

  private async topicLag(
    admin: Admin,
    groupId: string,
    topic: string,
  ): Promise<TopicLag> {
    const [latest, committed] = await Promise.all([
      admin.fetchTopicOffsets(topic),
      admin.fetchOffsets({ groupId, topics: [topic] }),
    ]);

    const committedByPartition = new Map<number, string>();
    for (const entry of committed) {
      for (const partition of entry.partitions) {
        committedByPartition.set(partition.partition, partition.offset);
      }
    }

    const partitions: PartitionLag[] = latest
      .map((entry) => {
        const rawCommitted = committedByPartition.get(entry.partition);
        // 카프카는 커밋 기록이 없으면 -1 을 준다. 이걸 0 으로 취급하면
        // "처음부터 다 처리했다" 는 뜻이 되어 Lag 이 실제보다 작게 보인다.
        const hasCommit =
          rawCommitted !== undefined && Number(rawCommitted) >= 0;
        const low = Number(entry.low);
        const high = Number(entry.high);
        const lag = hasCommit
          ? Math.max(high - Number(rawCommitted), 0)
          : Math.max(high - low, 0);

        return {
          partition: entry.partition,
          latestOffset: entry.high,
          committedOffset: hasCommit ? rawCommitted! : null,
          lag,
        };
      })
      .sort((a, b) => a.partition - b.partition);

    return {
      topic,
      totalLag: partitions.reduce((sum, p) => sum + p.lag, 0),
      partitions,
    };
  }

  /**
   * 오프셋을 되감는다. 카프카를 쓰는 가장 큰 이유다.
   *
   * 그룹에 활성 멤버가 있으면 카프카가 거부한다. 컨슈머를 먼저 멈춰야 한다.
   * 그 사실을 조용히 삼키지 않고 그대로 알린다.
   */
  async resetOffsets(
    groupId: string,
    topic: string,
    target: 'earliest' | 'latest',
  ): Promise<{
    groupId: string;
    topic: string;
    target: string;
    /** 실제로 넣은 파티션별 위치. 어디로 갔는지 확인용. */
    offsets: { partition: number; offset: string }[];
  }> {
    return this.withAdmin(async (admin) => {
      // 파티션마다 갈 위치를 직접 계산해서 명시적으로 넣는다.
      //
      // kafkajs 의 resetOffsets 는 커밋 기록을 '지운다'. 그러면 컨슈머는
      // 커밋이 없는 것으로 보고 자기 설정(fromBeginning)을 따르는데,
      // 그게 false 면 최신부터 읽어서 과거를 하나도 다시 안 읽는다.
      // 되감기는 성공했다고 나오는데 재처리는 안 되는 상태가 된다.
      //
      // low 는 아직 남아 있는 가장 오래된 위치다. retention 으로 지워진
      // 구간이 있으면 0 이 아니라 그 지점부터다.
      const topicOffsets = await admin.fetchTopicOffsets(topic);
      const offsets = topicOffsets.map((entry) => ({
        partition: entry.partition,
        offset: target === 'earliest' ? entry.low : entry.high,
      }));

      try {
        await admin.setOffsets({ groupId, topic, partitions: offsets });
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        throw new Error(
          `오프셋 되감기 실패 (그룹=${groupId} 토픽=${topic}): ${reason}. ` +
            '이 그룹의 컨슈머가 모두 멈춰 있어야 되감을 수 있습니다.',
        );
      }

      this.logger.log(
        `오프셋 되감기 그룹=${groupId} 토픽=${topic} 대상=${target} ` +
          `(${offsets.map((o) => `p${o.partition}:${o.offset}`).join(' ')})`,
      );
      return { groupId, topic, target, offsets };
    });
  }
}
