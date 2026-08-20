import { QueueTask } from './queue-task.interface';

/**
 * 워커 백엔드 어댑터 인터페이스.
 *
 * 작업(QueueTask)을 실제로 실행하는 백엔드를 추상화한다.
 * 구현체는 Node Worker Thread 풀, gRPC Go 사이드카, Kafka 프로듀서 등이 될 수 있다.
 *
 * fire-and-forget 백엔드(Kafka)와 요청-응답 백엔드(Node/gRPC)를 한 인터페이스로
 * 다루기 위해, 결과 타입은 두 케이스를 모두 담을 수 있게 정의한다.
 */
export interface WorkerBackend {
  /**
   * 백엔드 이름. 로그·통계 용도.
   */
  readonly name: string;

  /**
   * 작업 실행.
   *
   * 요청-응답 백엔드는 완료 결과를 리턴한다.
   * fire-and-forget 백엔드(Kafka)는 발행 접수 응답을 리턴한다.
   */
  execute(task: QueueTask): Promise<WorkerBackendResult>;

  /**
   * 백엔드 헬스체크. 통계·상태 조회 용도.
   */
  healthCheck?(): Promise<boolean>;
}

/**
 * 워커 백엔드 실행 결과.
 *
 * mode:
 *   'sync'  - 요청-응답. result에 실제 처리 결과 포함
 *   'async' - fire-and-forget. dispatch에 발행 좌표(파티션·오프셋) 포함
 */
export type WorkerBackendResult =
  | {
      mode: 'sync';
      taskId: string;
      success: boolean;
      result: unknown;
      error?: string;
      durationMs: number;
      backend: string;
    }
  | {
      mode: 'async';
      taskId: string;
      dispatch: {
        topic: string;
        partition: number;
        offset: string;
      };
      backend: string;
    };
