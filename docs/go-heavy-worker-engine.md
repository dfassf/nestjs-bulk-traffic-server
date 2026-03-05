# Go Heavy Worker Engine 설계 문서

## 개요

기존 NestJS Worker Thread 기반 처리 엔진과 병행하여, Go goroutine 기반 Heavy Worker Engine을 사이드카로 운용한다.
`WORKER_ENGINE` 환경변수로 엔진을 선택하거나, 벤치마크 모드로 양쪽을 동시 실행하여 성능을 비교한다.

## 아키텍처

```
Client Request
      |
NestJS API Gateway (기존)
      |
  TaskRouter  ← WORKER_ENGINE=node|go|both
    /    \
   v      v
Node Worker    Go Heavy Worker Engine
Thread Pool    (gRPC sidecar, :50051)
(기존)          |
              Task Router
            /    |     \
        Pool A  Pool B  Pool C
        CPU집약  I/O집약  배치처리
```

### 엔진 선택 모드

| `WORKER_ENGINE` | 동작 |
|-----------------|------|
| `node` (기본) | 기존 Worker Thread 풀 사용. Go 엔진 미실행 |
| `go` | Go 사이드카로 모든 작업 전달. Node Worker 비활성 |
| `both` | 동일 작업을 양쪽에 동시 dispatch. 먼저 완료된 결과를 클라이언트에 반환. 벤치마크 통계 수집 |

## Go Engine 내부 구조

### Task Router

요청의 `workloadType`을 기반으로 적절한 Pool로 분배한다.

| workloadType | Pool | 예시 |
|-------------|------|------|
| `cpu` | Pool A (CPU 집약) | 이미지 처리, 암호화, 파싱 |
| `io` | Pool B (I/O 집약) | 크롤링, API 호출, 파일 I/O |
| `batch` | Pool C (배치 처리) | 대량 메일, 데이터 집계, 리포트 생성 |
| `memory` | Pool A (fallback) | 메모리 집약 작업 |
| `custom` | Pool A (fallback) | 사용자 정의 작업 |

### Pool 설계

```go
type Pool struct {
    Name         string
    Workers      int           // goroutine 수
    MaxQueue     int           // 대기 큐 상한
    RateLimit    rate.Limiter  // 초당 요청 제한
    Timeout      time.Duration // 작업별 타임아웃
    RetryPolicy  RetryConfig   // 재시도 + backoff
}
```

기본 설정:

| Pool | Workers | MaxQueue | RateLimit | Timeout |
|------|---------|----------|-----------|---------|
| A (CPU) | `GOMAXPROCS` | 1000 | 없음 | 30s |
| B (I/O) | 100 | 5000 | 100/s | 60s |
| C (Batch) | 10 | 500 | 10/s | 300s |

### 공통 기능

- **Worker Pool**: `pool.Submit(task)` → goroutine 수 제어 (semaphore 패턴)
- **Rate Limiter**: `golang.org/x/time/rate` 기반 초당 요청 제한
- **Retry**: 지수 backoff + jitter. 최대 3회
- **Timeout**: `context.WithTimeout` 작업별 개별 타임아웃
- **Priority Queue**: heap 기반 우선순위 큐 (high > normal > low)
- **Health Check**: `/health` gRPC health check + HTTP fallback
- **Metrics**: Prometheus 호환. 처리량/지연시간/에러율/큐 깊이

## 통신 프로토콜

### gRPC (NestJS ↔ Go)

```protobuf
syntax = "proto3";
package worker;

service WorkerEngine {
  rpc Execute (TaskRequest) returns (TaskResponse);
  rpc ExecuteStream (TaskRequest) returns (stream TaskProgress);
  rpc GetStats (Empty) returns (EngineStats);
  rpc HealthCheck (Empty) returns (HealthResponse);
}

message TaskRequest {
  string task_id = 1;
  string workload_type = 2;  // cpu | io | batch | memory | custom
  int32 priority = 3;        // >= 5: high, >= 0: normal, < 0: low
  bytes payload = 4;
  int64 timeout_ms = 5;
  map<string, string> metadata = 6;
}

message TaskResponse {
  string task_id = 1;
  bool success = 2;
  bytes result = 3;
  string error = 4;
  int64 duration_ms = 5;
  string engine = 6;         // "go"
}

message TaskProgress {
  string task_id = 1;
  float progress = 2;        // 0.0 ~ 1.0
  string status = 3;
}

message EngineStats {
  int64 total_processed = 1;
  int64 total_failed = 2;
  int64 active_tasks = 3;
  map<string, PoolStats> pools = 4;
}

message PoolStats {
  int32 active_workers = 1;
  int32 queue_length = 2;
  int64 processed = 3;
  int64 failed = 4;
  double avg_latency_ms = 5;
  double p99_latency_ms = 6;
}

message HealthResponse {
  bool healthy = 1;
  string version = 2;
  int64 uptime_seconds = 3;
}

message Empty {}
```

## NestJS 측 변경사항

### 1. 새 파일: `src/queue/engine-router.service.ts`

TaskRouter 역할. `WORKER_ENGINE` 값에 따라 Node Worker 또는 Go Engine으로 라우팅.

```typescript
// 핵심 로직
async dispatch(task: QueueTask): Promise<TaskResult> {
  const engine = this.configService.get('WORKER_ENGINE', 'node');

  switch (engine) {
    case 'node':
      return this.workerPoolService.execute(task);
    case 'go':
      return this.goEngineClient.execute(task);
    case 'both':
      return this.dispatchBoth(task);
  }
}

// 벤치마크 모드: 양쪽 동시 실행, 먼저 온 결과 반환
private async dispatchBoth(task: QueueTask): Promise<TaskResult> {
  const [nodeResult, goResult] = await Promise.allSettled([
    this.workerPoolService.execute(task),
    this.goEngineClient.execute(task),
  ]);
  this.benchmarkService.record(task, nodeResult, goResult);
  // 먼저 성공한 결과 반환
  if (nodeResult.status === 'fulfilled') return nodeResult.value;
  if (goResult.status === 'fulfilled') return goResult.value;
  throw (nodeResult as PromiseRejectedResult).reason;
}
```

### 2. 새 파일: `src/queue/go-engine.client.ts`

gRPC 클라이언트. Go 사이드카와 통신.

### 3. 새 파일: `src/queue/benchmark.service.ts`

`both` 모드에서 양쪽 결과를 수집하고 통계를 제공.

### 4. 새 엔드포인트: `GET /benchmark-stats`

```json
{
  "totalComparisons": 1500,
  "nodeWins": 420,
  "goWins": 1080,
  "summary": {
    "node": {
      "avgLatencyMs": 145.2,
      "p99LatencyMs": 892.1,
      "errorRate": 0.02,
      "throughput": "32.1 tasks/s"
    },
    "go": {
      "avgLatencyMs": 28.7,
      "p99LatencyMs": 112.4,
      "errorRate": 0.005,
      "throughput": "156.8 tasks/s"
    }
  },
  "byWorkloadType": {
    "cpu": { "nodeAvg": 230.1, "goAvg": 15.3, "winner": "go" },
    "io": { "nodeAvg": 89.4, "goAvg": 42.1, "winner": "go" },
    "batch": { "nodeAvg": 1200.5, "goAvg": 340.2, "winner": "go" }
  }
}
```

## 환경 변수 (추가)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `WORKER_ENGINE` | `node` | `node` / `go` / `both` |
| `GO_ENGINE_HOST` | `localhost` | Go 엔진 gRPC 호스트 |
| `GO_ENGINE_PORT` | `50051` | Go 엔진 gRPC 포트 |
| `GO_ENGINE_AUTO_START` | `false` | NestJS 시작 시 Go 바이너리 자동 실행 여부 |
| `GO_POOL_A_WORKERS` | CPU 코어 수 | Pool A goroutine 수 |
| `GO_POOL_B_WORKERS` | `100` | Pool B goroutine 수 |
| `GO_POOL_C_WORKERS` | `10` | Pool C goroutine 수 |
| `GO_POOL_B_RATE_LIMIT` | `100` | Pool B 초당 요청 제한 |
| `GO_POOL_C_RATE_LIMIT` | `10` | Pool C 초당 요청 제한 |
| `BENCHMARK_SAMPLE_RATE` | `1.0` | both 모드에서 샘플링 비율 (0.0~1.0) |

## 디렉토리 구조 (추가)

```
nestjs-bulk-traffic-server/
├── src/                        # NestJS (기존)
│   └── queue/
│       ├── engine-router.service.ts   # 신규: 엔진 라우터
│       ├── go-engine.client.ts        # 신규: gRPC 클라이언트
│       ├── benchmark.service.ts       # 신규: 벤치마크 통계
│       └── ...
├── go-engine/                  # 신규: Go Heavy Worker Engine
│   ├── cmd/
│   │   └── engine/
│   │       └── main.go
│   ├── internal/
│   │   ├── pool/
│   │   │   ├── pool.go         # goroutine pool (semaphore)
│   │   │   ├── priority.go     # heap 기반 우선순위 큐
│   │   │   └── pool_test.go
│   │   ├── router/
│   │   │   └── router.go       # workloadType → pool 분배
│   │   ├── worker/
│   │   │   ├── cpu.go          # CPU 집약 작업 핸들러
│   │   │   ├── io.go           # I/O 집약 작업 핸들러
│   │   │   └── batch.go        # 배치 작업 핸들러
│   │   ├── metrics/
│   │   │   └── metrics.go      # Prometheus 메트릭
│   │   └── server/
│   │       └── grpc.go         # gRPC 서버
│   ├── proto/
│   │   └── worker.proto
│   ├── go.mod
│   ├── go.sum
│   ├── Dockerfile
│   └── Makefile
├── proto/                      # 공유 proto 정의
│   └── worker.proto
├── docker-compose.yml          # NestJS + Go 동시 실행
├── benchmark/                  # 벤치마크 스크립트
│   ├── load-test.sh            # k6/wrk 기반 부하 테스트
│   └── compare.sh              # 결과 비교 스크립트
└── docs/
    └── go-heavy-worker-engine.md  # 이 문서
```

## 실행 방법

### 개발 (Node only, 기본)
```bash
npm run start:dev
```

### 개발 (Go engine)
```bash
# 터미널 1: Go 엔진
cd go-engine && go run cmd/engine/main.go

# 터미널 2: NestJS
WORKER_ENGINE=go npm run start:dev
```

### 벤치마크 모드
```bash
# docker-compose로 양쪽 동시 실행
WORKER_ENGINE=both docker-compose up

# 부하 테스트
cd benchmark && ./load-test.sh

# 결과 확인
curl localhost:3000/benchmark-stats
```

### Docker Compose
```yaml
services:
  api:
    build: .
    ports:
      - "3000:3000"
    environment:
      - WORKER_ENGINE=both
      - GO_ENGINE_HOST=go-engine
      - GO_ENGINE_PORT=50051
    depends_on:
      - go-engine

  go-engine:
    build: ./go-engine
    ports:
      - "50051:50051"
    environment:
      - GO_POOL_A_WORKERS=8
      - GO_POOL_B_WORKERS=100
      - GO_POOL_C_WORKERS=10
```

## 구현 순서

1. proto 정의 + Go 프로젝트 초기화 (`go-engine/`)
2. Go: Pool + Priority Queue + Task Router 구현
3. Go: gRPC 서버 + Health Check
4. NestJS: `go-engine.client.ts` (gRPC 클라이언트)
5. NestJS: `engine-router.service.ts` (엔진 라우터)
6. NestJS: `benchmark.service.ts` + `/benchmark-stats`
7. Docker Compose + 부하 테스트 스크립트
8. 벤치마크 실행 및 결과 문서화
