import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { QueueTask } from './interfaces/queue-task.interface';

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

    const workloadType = task.workloadType || 'cpu';
    const payload = JSON.stringify(task.params || {});

    const request = {
      taskId: String(task.id),
      workloadType,
      priority: task.priority,
      payload: Buffer.from(payload),
      timeoutMs: task.timeout || 30000,
      metadata: {},
    };

    return new Promise<GoEngineResult>((resolve, reject) => {
      const deadline = new Date(Date.now() + (task.timeout || 30000));

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
