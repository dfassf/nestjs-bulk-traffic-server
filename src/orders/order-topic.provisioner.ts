import {
  Inject,
  Injectable,
  Logger,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import { Admin, Kafka, logLevel } from 'kafkajs';
import { ORDER_TOPICS } from './order-events';
import { readKafkaBrokersEnv, readPositiveIntEnv } from '../queue/utils/env';

export const TOPIC_PROVISIONER_CONFIG = Symbol('TOPIC_PROVISIONER_CONFIG');

export interface TopicProvisionerConfig {
  brokers: string[];
  clientId: string;
  enabled: boolean;
  /** 새로 만들 토픽의 파티션 수. 실험에서 병렬성 상한을 정하는 값이다. */
  partitions: number;
  /** 단일 노드 개발 환경이라 1. 노드를 늘리면 함께 올린다. */
  replicationFactor: number;
}

export function topicProvisionerConfigFromEnv(
  enabled: boolean,
): TopicProvisionerConfig {
  return {
    brokers: readKafkaBrokersEnv(),
    clientId: process.env.ORDER_TOPIC_ADMIN_CLIENT_ID ?? 'order-topic-admin',
    enabled,
    partitions: readPositiveIntEnv('ORDER_TOPIC_PARTITIONS', 6),
    replicationFactor: readPositiveIntEnv('ORDER_TOPIC_REPLICATION_FACTOR', 1),
  };
}

export interface ProvisionReport {
  created: string[];
  existing: string[];
  /** 이미 있지만 파티션이 모자란 토픽. 실험이 성립하지 않으므로 경고 대상. */
  underPartitioned: { topic: string; actual: number; expected: number }[];
}

/**
 * 주문 토픽을 앱 부팅 시 명시적으로 만든다.
 *
 * 브로커의 자동 생성에 맡기면 파티션 1개짜리가 조용히 생긴다(기본값).
 * 파티션이 1개면 키 라우팅·순서 붕괴·컨슈머 분배 실험이 전부 성립하지 않는데,
 * 발행은 정상으로 보여서 알아차리기 어렵다. 실제로 그 일이 있었다.
 */
@Injectable()
export class OrderTopicProvisioner implements OnModuleInit {
  private readonly logger = new Logger(OrderTopicProvisioner.name);
  private readonly config: TopicProvisionerConfig;
  private lastReport: ProvisionReport | null = null;

  constructor(
    @Optional()
    @Inject(TOPIC_PROVISIONER_CONFIG)
    config?: TopicProvisionerConfig,
  ) {
    this.config = config ?? topicProvisionerConfigFromEnv(true);
  }

  async onModuleInit(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.log('주문 토픽 준비를 건너뜁니다(카프카 모드 아님).');
      return;
    }
    await this.provision();
  }

  getLastReport(): ProvisionReport | null {
    return this.lastReport;
  }

  getConfig(): Readonly<TopicProvisionerConfig> {
    return this.config;
  }

  async provision(): Promise<ProvisionReport> {
    const { brokers, clientId, partitions, replicationFactor } = this.config;
    if (brokers.length === 0) {
      throw new Error(
        'Kafka 브로커 주소가 비어 있습니다. KAFKA_BROKERS 를 확인하세요.',
      );
    }

    const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.NOTHING });
    const admin = kafka.admin();
    await admin.connect();

    try {
      const report = await this.ensureTopics(
        admin,
        partitions,
        replicationFactor,
      );
      this.lastReport = report;
      this.logReport(report, partitions);
      return report;
    } finally {
      await admin.disconnect();
    }
  }

  private async ensureTopics(
    admin: Admin,
    partitions: number,
    replicationFactor: number,
  ): Promise<ProvisionReport> {
    const wanted = Object.values(ORDER_TOPICS);
    const existingNames = await admin.listTopics();

    const missing = wanted.filter((topic) => !existingNames.includes(topic));
    const existing = wanted.filter((topic) => existingNames.includes(topic));

    if (missing.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({
          topic,
          numPartitions: partitions,
          replicationFactor,
        })),
      });
    }

    return {
      created: missing,
      existing,
      underPartitioned: await this.findUnderPartitioned(
        admin,
        existing,
        partitions,
      ),
    };
  }

  /**
   * 이미 있는 토픽 중 파티션이 모자란 것을 찾는다.
   *
   * 자동으로 늘리지 않는다. 파티션을 늘리면 같은 키가 다른 파티션으로 가서
   * 기존 메시지의 순서 보장이 깨지기 때문이다. 사람이 판단할 일이다.
   */
  private async findUnderPartitioned(
    admin: Admin,
    topics: string[],
    expected: number,
  ): Promise<ProvisionReport['underPartitioned']> {
    if (topics.length === 0) return [];

    const metadata = await admin.fetchTopicMetadata({ topics });
    return metadata.topics
      .map((meta) => ({
        topic: meta.name,
        actual: meta.partitions.length,
        expected,
      }))
      .filter((entry) => entry.actual < entry.expected);
  }

  private logReport(report: ProvisionReport, partitions: number): void {
    if (report.created.length > 0) {
      this.logger.log(
        `주문 토픽 생성(파티션 ${partitions}개): ${report.created.join(', ')}`,
      );
    }
    if (report.existing.length > 0) {
      this.logger.log(`이미 있는 주문 토픽: ${report.existing.join(', ')}`);
    }

    // 파티션이 모자라면 실험이 성립하지 않는다. 조용히 넘기면 결과를 잘못 읽는다.
    for (const entry of report.underPartitioned) {
      this.logger.warn(
        `${entry.topic} 파티션이 ${entry.actual}개뿐입니다(기대 ${entry.expected}개). ` +
          '파티션 분배·순서 실험이 의도대로 동작하지 않습니다. ' +
          '토픽을 지우고 다시 만들거나 파티션을 늘리세요.',
      );
    }
  }
}
