import { readKafkaBrokersEnv, readPositiveIntEnv } from '../../queue/utils/env';
import { ORDER_TOPICS } from '../order-events';

/**
 * 오프셋을 언제 커밋할지.
 *
 * 카프카는 "어디까지 읽었나" 를 오프셋으로 기억한다. 이걸 언제 기록하느냐에 따라
 * 프로세스가 갑자기 죽었을 때 결과가 갈린다.
 *
 *   after-process  처리 후 커밋. 처리했는데 커밋 전에 죽으면 재시작 후 다시 처리(중복).
 *                  실무 기본값이고, 최소 한 번은 처리된다(at-least-once).
 *   before-process 처리 전 커밋. 커밋했는데 처리 전에 죽으면 그 건은 영영 처리 안 됨(유실).
 *                  최대 한 번만 처리된다(at-most-once).
 *
 * 둘 다 실험 대상이다. 유실과 중복 중 무엇을 감수할지가 설계 선택이라는 걸 보려고 둔다.
 */
export type CommitMode = 'after-process' | 'before-process';

export interface ConsumerConfig {
  brokers: string[];
  clientId: string;
  /** 컨슈머 그룹. 같은 그룹끼리 파티션을 나눠 갖는다. */
  groupId: string;
  topics: string[];
  /** 이 프로세스에서 띄울 컨슈머 개수. 파티션 분배를 볼 때 늘린다. */
  instances: number;
  /** 한 건 처리에 걸리는 시간을 흉내낸다. Lag 을 쌓을 때 올린다. */
  processingDelayMs: number;
  commitMode: CommitMode;
  /**
   * 처리와 커밋 사이에 두는 지연.
   *
   * 이 구간에서 프로세스를 죽이면 "처리했지만 커밋 못 한" 상태가 만들어진다.
   * 그냥 두면 이 창이 너무 짧아 재현이 어렵다. 일부러 넓혀서 100% 재현한다.
   */
  commitDelayMs: number;
  /** 처음부터 읽을지, 최신부터 읽을지. 새 그룹으로 과거를 다시 볼 때 쓴다. */
  fromBeginning: boolean;
  /** 이 건수만큼 처리하면 스스로 죽는다. 중복 실험 자동화용. 0이면 안 죽는다. */
  crashAfter: number;
}

const DEFAULT_TOPICS = Object.values(ORDER_TOPICS);

function parseCommitMode(raw: string | undefined): CommitMode {
  if (raw === undefined || raw.trim() === '') return 'after-process';

  const normalized = raw.trim().toLowerCase();
  if (normalized === 'after-process' || normalized === 'before-process') {
    return normalized;
  }

  // 오타를 기본값으로 흡수하면 어떤 방식으로 돌고 있는지 모른 채 실험하게 된다.
  throw new Error(
    `CONSUMER_COMMIT_MODE 는 after-process 또는 before-process 여야 합니다. 현재 값: ${raw}`,
  );
}

/** 0 이상 정수 환경변수. readPositiveIntEnv 는 0을 허용하지 않아 따로 둔다. */
function readNonNegativeIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} 는 0 이상 정수여야 합니다. 현재 값: ${raw}`);
  }
  return parsed;
}

function parseTopics(raw: string | undefined): string[] {
  if (!raw || raw.trim() === '') return DEFAULT_TOPICS;

  const topics = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  if (topics.length === 0) {
    throw new Error('CONSUMER_TOPICS 에 구독할 토픽이 하나도 없습니다.');
  }
  return topics;
}

export function consumerConfigFromEnv(): ConsumerConfig {
  return {
    brokers: readKafkaBrokersEnv(),
    clientId: process.env.CONSUMER_CLIENT_ID ?? 'order-consumer',
    groupId: process.env.CONSUMER_GROUP_ID ?? 'order-processor',
    topics: parseTopics(process.env.CONSUMER_TOPICS),
    instances: readPositiveIntEnv('CONSUMER_COUNT', 1),
    processingDelayMs: readNonNegativeIntEnv('CONSUMER_DELAY_MS', 0),
    commitMode: parseCommitMode(process.env.CONSUMER_COMMIT_MODE),
    commitDelayMs: readNonNegativeIntEnv('CONSUMER_COMMIT_DELAY_MS', 0),
    fromBeginning: process.env.CONSUMER_FROM_BEGINNING === 'true',
    crashAfter: readNonNegativeIntEnv('CONSUMER_CRASH_AFTER', 0),
  };
}

export function describeConfig(config: ConsumerConfig): string {
  const parts = [
    `그룹=${config.groupId}`,
    `인스턴스=${config.instances}`,
    `커밋=${config.commitMode}`,
  ];
  if (config.processingDelayMs > 0)
    parts.push(`처리지연=${config.processingDelayMs}ms`);
  if (config.commitDelayMs > 0)
    parts.push(`커밋지연=${config.commitDelayMs}ms`);
  if (config.fromBeginning) parts.push('처음부터');
  if (config.crashAfter > 0) parts.push(`${config.crashAfter}건 후 강제종료`);
  return parts.join(' ');
}
