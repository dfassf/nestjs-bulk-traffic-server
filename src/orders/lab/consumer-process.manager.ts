import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { ChildProcess, spawn } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import { CommitMode } from '../consumer/consumer-config';

export interface SpawnConsumerOptions {
  groupId?: string;
  instances?: number;
  processingDelayMs?: number;
  commitMode?: CommitMode;
  commitDelayMs?: number;
  fromBeginning?: boolean;
  crashAfter?: number;
  /** 카프카가 급사한 컨슈머를 죽은 것으로 판정하기까지 기다리는 시간(ms). */
  sessionTimeoutMs?: number;
}

export interface ConsumerProcessInfo {
  pid: number;
  groupId: string;
  instances: number;
  startedAt: number;
  options: Required<SpawnConsumerOptions>;
  alive: boolean;
  /** 죽은 경우 종료 코드·신호. 왜 죽었는지 구분하려고 남긴다. */
  exitCode: number | null;
  exitSignal: string | null;
  recentLogs: string[];
}

/** 테스트에서 가짜 스크립트를 넣기 위한 주입 토큰. 평소에는 비어 있다. */
export const CONSUMER_SCRIPT_PATH = Symbol('CONSUMER_SCRIPT_PATH');

const MAX_INSTANCES = 20;
const MAX_DELAY_MS = 60_000;
const MAX_LOG_LINES = 50;

// 브로커의 group.min/max.session.timeout.ms 기본 범위(6초~30분).
// 이 밖의 값을 주면 컨슈머가 그룹 참여를 거부당한다.
const MIN_SESSION_TIMEOUT_MS = 6_000;
const MAX_SESSION_TIMEOUT_MS = 1_800_000;
const COMMIT_MODES: CommitMode[] = ['after-process', 'before-process'];

/**
 * 컨슈머 프로세스를 띄우고 죽인다.
 *
 * 실험 조작판이 컨슈머 개수를 바꾸거나 강제 종료를 시키려면 서버가
 * 자식 프로세스를 다뤄야 한다. 서버가 임의 명령을 실행하는 건 위험하므로
 * 실행 대상을 dist/consumer.js 하나로 고정하고, 인자는 환경변수로만 넘기며
 * 값도 전부 검증한다. 셸을 거치지 않아 명령 주입도 불가능하다.
 */
@Injectable()
export class ConsumerProcessManager implements OnModuleDestroy {
  private readonly logger = new Logger(ConsumerProcessManager.name);
  private readonly processes = new Map<number, ConsumerProcessInfo>();
  private readonly handles = new Map<number, ChildProcess>();

  constructor(
    @Optional()
    @Inject(CONSUMER_SCRIPT_PATH)
    private readonly consumerScriptPath?: string,
  ) {}

  async onModuleDestroy(): Promise<void> {
    // 서버가 내려가면 자식도 정리한다. 안 그러면 고아 프로세스가 남는다.
    for (const pid of this.handles.keys()) {
      this.stop(pid, 'SIGTERM');
    }
  }

  private resolveScriptPath(): string {
    const scriptPath =
      this.consumerScriptPath ??
      path.resolve(process.cwd(), 'dist/consumer.js');

    if (!fs.existsSync(scriptPath)) {
      throw new Error(
        `컨슈머 스크립트를 찾을 수 없습니다: ${scriptPath}. npm run build 를 먼저 실행하세요.`,
      );
    }
    return scriptPath;
  }

  /** 조작판에서 온 값을 검증한다. 잘못된 값을 기본값으로 흡수하지 않는다. */
  private normalize(
    options: SpawnConsumerOptions,
  ): Required<SpawnConsumerOptions> {
    const instances = options.instances ?? 1;
    if (
      !Number.isInteger(instances) ||
      instances < 1 ||
      instances > MAX_INSTANCES
    ) {
      throw new Error(
        `컨슈머 개수는 1~${MAX_INSTANCES} 사이 정수여야 합니다: ${instances}`,
      );
    }

    const processingDelayMs = options.processingDelayMs ?? 0;
    if (
      !Number.isInteger(processingDelayMs) ||
      processingDelayMs < 0 ||
      processingDelayMs > MAX_DELAY_MS
    ) {
      throw new Error(
        `처리 지연은 0~${MAX_DELAY_MS}ms 사이여야 합니다: ${processingDelayMs}`,
      );
    }

    const commitDelayMs = options.commitDelayMs ?? 0;
    if (
      !Number.isInteger(commitDelayMs) ||
      commitDelayMs < 0 ||
      commitDelayMs > MAX_DELAY_MS
    ) {
      throw new Error(
        `커밋 지연은 0~${MAX_DELAY_MS}ms 사이여야 합니다: ${commitDelayMs}`,
      );
    }

    const crashAfter = options.crashAfter ?? 0;
    if (!Number.isInteger(crashAfter) || crashAfter < 0) {
      throw new Error(`강제 종료 건수는 0 이상 정수여야 합니다: ${crashAfter}`);
    }

    // 브로커가 받아주는 범위를 벗어나면 컨슈머가 그룹 참여 자체를 거부당한다.
    // 조용히 기본값으로 바꾸면 어떤 값으로 실험했는지 모르게 된다.
    const sessionTimeoutMs = options.sessionTimeoutMs ?? 60000;
    if (
      !Number.isInteger(sessionTimeoutMs) ||
      sessionTimeoutMs < MIN_SESSION_TIMEOUT_MS ||
      sessionTimeoutMs > MAX_SESSION_TIMEOUT_MS
    ) {
      throw new Error(
        `세션 만료 시간은 ${MIN_SESSION_TIMEOUT_MS}~${MAX_SESSION_TIMEOUT_MS}ms 사이여야 합니다: ${sessionTimeoutMs}`,
      );
    }

    const commitMode = options.commitMode ?? 'after-process';
    if (!COMMIT_MODES.includes(commitMode)) {
      throw new Error(
        `커밋 방식은 ${COMMIT_MODES.join(' 또는 ')} 여야 합니다: ${commitMode}`,
      );
    }

    const groupId = (options.groupId ?? 'order-processor').trim();
    // 그룹 이름이 비면 카프카가 거부한다. 조용히 기본값을 쓰면 어느 그룹인지 헷갈린다.
    if (groupId === '') {
      throw new Error('컨슈머 그룹 이름이 비어 있습니다.');
    }

    return {
      groupId,
      instances,
      processingDelayMs,
      commitMode,
      commitDelayMs,
      fromBeginning: options.fromBeginning ?? false,
      crashAfter,
      sessionTimeoutMs,
    };
  }

  spawnConsumer(options: SpawnConsumerOptions = {}): ConsumerProcessInfo {
    const scriptPath = this.resolveScriptPath();
    const normalized = this.normalize(options);

    const child = spawn(process.execPath, [scriptPath], {
      // 셸을 거치지 않는다. 값에 무엇이 들어와도 명령으로 해석되지 않는다.
      shell: false,
      env: {
        ...process.env,
        // 서버가 보는 DB 를 자식도 똑같이 보게 명시한다. 상속에만 맡기면
        // 서버가 이 값 없이 떠 있을 때 서로 다른 파일을 보게 되고,
        // 조작판의 중복 건수가 0 으로 보인다(기록은 다른 파일에 쌓이는데).
        ORDER_DB_PATH: process.env.ORDER_DB_PATH ?? '.orders.sqlite',
        CONSUMER_GROUP_ID: normalized.groupId,
        CONSUMER_COUNT: String(normalized.instances),
        CONSUMER_DELAY_MS: String(normalized.processingDelayMs),
        CONSUMER_COMMIT_MODE: normalized.commitMode,
        CONSUMER_COMMIT_DELAY_MS: String(normalized.commitDelayMs),
        CONSUMER_FROM_BEGINNING: String(normalized.fromBeginning),
        CONSUMER_CRASH_AFTER: String(normalized.crashAfter),
        CONSUMER_SESSION_TIMEOUT_MS: String(normalized.sessionTimeoutMs),
        CONSUMER_REPORT_INTERVAL_MS: '5000',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    if (child.pid === undefined) {
      throw new Error('컨슈머 프로세스를 띄우지 못했습니다.');
    }

    const info: ConsumerProcessInfo = {
      pid: child.pid,
      groupId: normalized.groupId,
      instances: normalized.instances,
      startedAt: Date.now(),
      options: normalized,
      alive: true,
      exitCode: null,
      exitSignal: null,
      recentLogs: [],
    };

    this.processes.set(child.pid, info);
    this.handles.set(child.pid, child);
    this.attachListeners(child, info);

    this.logger.log(
      `컨슈머 프로세스 시작 pid=${child.pid} 그룹=${normalized.groupId} 인스턴스=${normalized.instances}`,
    );
    return info;
  }

  private attachListeners(
    child: ChildProcess,
    info: ConsumerProcessInfo,
  ): void {
    const collect = (chunk: Buffer) => {
      for (const line of chunk.toString().split('\n')) {
        if (line.trim() === '') continue;
        info.recentLogs.push(line);
        if (info.recentLogs.length > MAX_LOG_LINES) info.recentLogs.shift();
      }
    };

    child.stdout?.on('data', collect);
    child.stderr?.on('data', collect);

    child.on('exit', (code, signal) => {
      info.alive = false;
      info.exitCode = code;
      info.exitSignal = signal;
      this.handles.delete(info.pid);
      this.logger.log(
        `컨슈머 프로세스 종료 pid=${info.pid} code=${code} signal=${signal ?? '없음'}`,
      );
    });
  }

  /**
   * 프로세스를 종료한다.
   *
   * SIGKILL 은 프로세스가 정리할 틈 없이 즉시 죽는다. 오프셋 커밋도 못 하므로
   * 재시작하면 중복 처리가 관측된다. 실무에서 중복이 생기는 상황이 이것이다.
   * SIGTERM 은 정상 종료라 커밋하고 빠진다.
   */
  stop(pid: number, signal: NodeJS.Signals = 'SIGKILL'): ConsumerProcessInfo {
    const info = this.processes.get(pid);
    if (!info) {
      throw new Error(`그런 컨슈머 프로세스가 없습니다: pid=${pid}`);
    }

    const handle = this.handles.get(pid);
    if (!handle) {
      // 이미 죽은 프로세스에 신호를 보내면 다른 프로세스를 죽일 수도 있다.
      throw new Error(`이미 종료된 프로세스입니다: pid=${pid}`);
    }

    handle.kill(signal);
    this.logger.log(`컨슈머 프로세스에 ${signal} 전송 pid=${pid}`);
    return info;
  }

  stopAll(signal: NodeJS.Signals = 'SIGTERM'): number {
    const pids = [...this.handles.keys()];
    for (const pid of pids) {
      try {
        this.stop(pid, signal);
      } catch {
        // 종료 경합으로 이미 사라졌을 수 있다. 전체 정리를 막을 이유는 아니다.
      }
    }
    return pids.length;
  }

  list(): ConsumerProcessInfo[] {
    return [...this.processes.values()].sort(
      (a, b) => b.startedAt - a.startedAt,
    );
  }

  /** 종료된 기록을 지운다. 목록이 계속 늘어나는 걸 막는다. */
  clearFinished(): number {
    const finished = [...this.processes.values()].filter((info) => !info.alive);
    for (const info of finished) {
      this.processes.delete(info.pid);
    }
    return finished.length;
  }
}
