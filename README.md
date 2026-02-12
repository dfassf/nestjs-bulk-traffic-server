# nestjs-bulk-traffic-server

대용량 요청을 우선순위 큐로 흡수하고, CPU/메모리 집약 작업은 Worker Thread 풀로 오프로딩하는 NestJS 서버 골격입니다.

## 프로젝트 목적
- 트래픽 버스트 상황에서 요청을 즉시 실행하지 않고 큐에 적재해 안정적으로 처리
- 우선순위(high/normal/low)와 배치 처리로 처리량(throughput)과 응답 안정성 확보
- CPU/메모리 집약 작업을 Worker 풀로 분리해 메인 이벤트 루프 블로킹 완화
- 메모리 압박/큐 포화/타임아웃 상황에서 방어적으로 거부 및 정리

## 핵심 기능
- 전역 요청 큐잉 미들웨어 (`QueueMiddleware`)
- 3단계 우선순위 큐 + 배치 큐 (`QueueService`, `BatchService`)
- 메모리 압박 감지 및 저우선순위 선제 제거 (`MemoryService`)
- Worker Thread 풀 + 헬스체크 + 장애 워커 자동 교체 (`WorkerPoolService`)
- 큐 스냅샷 저장/복구(옵션, file strategy) (`PersistenceModule`)
- 운영 관측 엔드포인트 (`/health`, `/queue-stats`)

## 아키텍처 다이어그램
```mermaid
flowchart TD
  C[Client Request] --> M[QueueMiddleware]
  M -->|bypass| R[Route Handler]
  M -->|enqueue| Q[QueueService]

  Q --> HQ[High Queue]
  Q --> NQ[Normal Queue]
  Q --> LQ[Low Queue]
  Q --> BQ[Batch Queues]

  Q -->|CPU/MEMORY/CUSTOM| WP[WorkerPoolService]
  Q -->|general task| EX[Main Thread Execute]

  WP --> W1[Worker #1..N]
  W1 --> Q
  EX --> Q

  Q --> ST[/queue-stats]
  Q --> P[Queue Persistence]
  P --> F[(Snapshot File)]
```

## 요청 처리 흐름
1. 모든 요청(health/queue-stats 제외)이 `QueueMiddleware`를 통과합니다.
2. 미들웨어가 `method + path + body`를 분석해 `priority/category/size/workloadType`을 계산합니다.
3. `QueueService.enqueue()`가 요청을 큐 또는 배치 큐에 적재합니다.
4. `QueueService`는 이벤트 기반(`setImmediate`)으로 우선순위 큐를 소진하고, 폴링(`QUEUE_PROCESS_INTERVAL_MS`)은 fallback으로 동작합니다.
5. 작업 유형이 `cpu/memory/custom`이면 Worker 풀로 전달하고, 아니면 메인 스레드에서 실행합니다.
6. 완료/실패/타임아웃 결과를 요청 단위로 resolve/reject 후 통계 누적합니다.

## 큐/워커 동작 규칙
- 우선순위: `priority >= 5 => high`, `>= 0 => normal`, `< 0 => low`
- 메모리 압박(`memoryPressure=true`) 시 low 큐 처리를 일시 중단
- 큐 대기 타임아웃: `QUEUE_TASK_TIMEOUT_MS`
- 작업 실행 타임아웃: `QUEUE_EXECUTION_TIMEOUT_MS`
- 큐 포화 시 즉시 거부: `QUEUE_OVERFLOW_THRESHOLD`
- Worker 풀은 타입별 동시성 제한 적용
  - `WORKER_MAX_CPU_CONCURRENCY`
  - `WORKER_MAX_MEMORY_CONCURRENCY`
  - `WORKER_MAX_CUSTOM_CONCURRENCY`
- 알 수 없는 workloadType은 일반 큐 fallback으로 처리하고 카운트(`workloadGeneralQueueFallbackCount`)에 누적

## 엔드포인트
- `GET /health`
  - 프로세스/메모리/시스템 상태 반환
- `GET /queue-stats`
  - 큐 길이, 처리/거부/타임아웃 통계, 워커풀 상태, 영속성 상태 반환

참고: 현재는 큐/워커 인프라 중심 골격 프로젝트입니다. 비즈니스 API는 추가 구현이 필요합니다.

## 실행 방법
```bash
npm install
cp .env.example .env
npm run start:dev
```

빌드/테스트:
```bash
npm run build
npm test
```

## 환경 변수
아래 키는 `.env.example` 기준입니다.

### 서버
- `PORT`: 서버 포트
- `ALLOWED_ORIGINS`: CORS 허용 origin(콤마 구분)
- `UV_THREADPOOL_SIZE`: libuv threadpool 크기
- `DISABLE_WORKERS`: `true`면 Worker 풀 비활성화
- `ALLOW_CUSTOM_WORKLOAD`: `true`일 때만 custom workload 허용

### 워커 풀
- `WORKER_POOL_SIZE`: Worker 개수
- `WORKER_MAX_CPU_CONCURRENCY`: CPU 타입 동시 실행 제한
- `WORKER_MAX_MEMORY_CONCURRENCY`: MEMORY 타입 동시 실행 제한
- `WORKER_MAX_CUSTOM_CONCURRENCY`: CUSTOM 타입 동시 실행 제한

### 큐 코어
- `QUEUE_CONCURRENT_TASKS`: 한 번에 큐에서 꺼내 dispatch하는 최대 작업 수
- `QUEUE_MAX_CONCURRENT_REQUESTS`: 전체 활성 작업 상한
- `QUEUE_TASK_TIMEOUT_MS`: 큐 대기 타임아웃
- `QUEUE_EXECUTION_TIMEOUT_MS`: 작업 실행 타임아웃
- `QUEUE_OVERFLOW_THRESHOLD`: 총 큐 길이 상한
- `QUEUE_PROCESS_INTERVAL_MS`: fallback 폴링 주기
- `QUEUE_MEMORY_CHECK_INTERVAL_MS`: 메모리 체크 주기
- `QUEUE_STATS_LOG_INTERVAL_MS`: 1분 통계 로그 주기

### 영속성
- `QUEUE_PERSISTENCE`: `none` 또는 `file`
- `QUEUE_SNAPSHOT_PATH`: 스냅샷 파일 경로
- `QUEUE_SNAPSHOT_INTERVAL_MS`: 스냅샷 저장 주기
- `QUEUE_SNAPSHOT_MAX_AGE_MS`: 만료된 스냅샷 무시 기준

## 운영 관측 포인트
- API: `GET /queue-stats`
  - `totalQueueLength`, `batchTaskCount`, `activeRequests`
  - `workerPool.activeByType`, `workerPool.queueByType`
  - `workloadGeneralQueueFallbackCount`
  - `persistence.lastSnapshotAt`
- 로그:
  - 큐 1분 통계 로그(`[1분 통계] ...`)
  - 메모리 압박/정상화 경고 로그
  - 워커 헬스체크 실패 및 재생성 로그

## 주의사항
- `ALLOW_CUSTOM_WORKLOAD=true`는 동적 코드 실행(`new Function`)을 허용하므로 내부망/통제 환경에서만 사용해야 합니다.
- 스냅샷은 통계/메타 복구용입니다. 대기 작업의 실행 함수는 직렬화할 수 없어 재실행 복구되지 않습니다.

