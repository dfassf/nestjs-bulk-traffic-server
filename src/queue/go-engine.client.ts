import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { QueueTask, WorkloadType } from './interfaces/queue-task.interface';

/** 작업에 타임아웃이 지정되지 않았을 때 쓰는 상한. 실행 시간 보호용이라 기본값이 정당하다. */
const DEFAULT_TASK_TIMEOUT_MS = 30000;

interface TaskResponse {
  taskId: string;
  success: boolean;
  result: Buffer;
  error: string;
  durationMs: number;
  engine: string;
}

export interface PoolStatsProto {
  activeWorkers: number;
  queueLength: number;
  processed: number;
  failed: number;
  avgLatencyMs: number;
  p99LatencyMs: number;
}

interface EngineStatsProto {
  totalProcessed: number;
  totalFailed: number;
  activeTasks: number;
  pools: Record<string, PoolStatsProto>;
}

interface HealthResponseProto {
  healthy: boolean;
  version: string;
  uptimeSeconds: number;
}

export interface GoEngineResult {
  taskId: string;
  success: boolean;
  result: unknown;
  error: string;
  durationMs: number;
  engine: string;
}

@Injectable()
export class GoEngineClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(GoEngineClient.name);
  private client: any;
  private connected = false;

  private readonly host = process.env.GO_ENGINE_HOST || 'localhost';
  private readonly port = parseInt(process.env.GO_ENGINE_PORT || '50051', 10);

  onModuleInit() {
    this.connect();
  }

  onModuleDestroy() {
    if (this.client) {
      this.client.close();
    }
  }

  private connect(): void {
    const protoPath = path.resolve(process.cwd(), 'proto/worker.proto');

    const packageDefinition = protoLoader.loadSync(protoPath, {
      keepCase: false,
      longs: Number,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const proto = grpc.loadPackageDefinition(packageDefinition) as any;
    const address = `${this.host}:${this.port}`;

    this.client = new proto.worker.WorkerEngine(
      address,
      grpc.credentials.createInsecure(),
    );

    // Test connection
    this.client.waitForReady(Date.now() + 5000, (err: Error | null) => {
      if (err) {
        this.logger.warn(`Go 엔진 연결 실패 (${address}): ${err.message}`);
        this.connected = false;
      } else {
        this.logger.log(`Go 엔진 연결 성공: ${address}`);
        this.connected = true;
      }
    });
  }

  isConnected(): boolean {
    return this.connected;
  }

  async execute(task: QueueTask): Promise<GoEngineResult> {
    if (!this.client) {
      throw new Error('Go 엔진 클라이언트가 초기화되지 않았습니다.');
    }

    // 원래 작업 종류를 그대로 넘긴다. 여기서 'cpu' 로 덮어쓰면 Go 엔진 로그에도
    // cpu 로만 남아, 종류가 빠진 작업이 얼마나 되는지 추적할 수 없다.
    // 미지정 처리(cpu 풀 배정)는 Go 라우터의 default 분기가 이미 맡는다.
    if (!task.workloadType) {
      this.logger.warn(
        `작업 ${task.id} 에 workloadType 이 없습니다. Go 엔진의 기본 풀로 배정됩니다.`,
      );
    }
    const workloadType = task.workloadType ?? WorkloadType.UNKNOWN;
    const payload = JSON.stringify(task.params ?? {});
    const timeoutMs = task.timeout ?? DEFAULT_TASK_TIMEOUT_MS;

    const request = {
      taskId: String(task.id),
      workloadType,
      priority: task.priority,
      payload: Buffer.from(payload),
      timeoutMs,
      metadata: {},
    };

    return new Promise<GoEngineResult>((resolve, reject) => {
      const deadline = new Date(Date.now() + timeoutMs);

      this.client.Execute(
        request,
        { deadline },
        (err: grpc.ServiceError | null, response: TaskResponse) => {
          if (err) {
            this.connected = false;
            reject(new Error(`Go 엔진 호출 실패: ${err.message}`));
            return;
          }

          this.connected = true;

          let parsedResult: unknown = null;
          if (response.result && response.result.length > 0) {
            try {
              parsedResult = JSON.parse(response.result.toString());
            } catch {
              parsedResult = response.result.toString();
            }
          }

          resolve({
            taskId: response.taskId,
            success: response.success,
            result: parsedResult,
            error: response.error,
            durationMs: response.durationMs,
            engine: response.engine,
          });
        },
      );
    });
  }

  async getStats(): Promise<EngineStatsProto> {
    if (!this.client) {
      throw new Error('Go 엔진 클라이언트가 초기화되지 않았습니다.');
    }

    return new Promise((resolve, reject) => {
      this.client.GetStats(
        {},
        { deadline: new Date(Date.now() + 5000) },
        (err: grpc.ServiceError | null, response: EngineStatsProto) => {
          if (err) {
            reject(new Error(`Go 엔진 통계 조회 실패: ${err.message}`));
            return;
          }
          resolve(response);
        },
      );
    });
  }

  async healthCheck(): Promise<HealthResponseProto> {
    if (!this.client) {
      throw new Error('Go 엔진 클라이언트가 초기화되지 않았습니다.');
    }

    return new Promise((resolve, reject) => {
      this.client.HealthCheck(
        {},
        { deadline: new Date(Date.now() + 5000) },
        (err: grpc.ServiceError | null, response: HealthResponseProto) => {
          if (err) {
            reject(new Error(`Go 엔진 헬스체크 실패: ${err.message}`));
            return;
          }
          resolve(response);
        },
      );
    });
  }
}
