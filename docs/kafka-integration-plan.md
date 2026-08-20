# Kafka 통합 도입 계획 (다음 세션 이어받기용)

작성일: 2026-07-05
작성 맥락: 이력서·점핏 연봉 리포트 분석 후, Kafka 스택 도입이 학습 ROI·연봉 밴드 진입 관점에서 최상위로 판정됨. 이 프로젝트가 카프카 붙이기에 가장 적합한 자산이라 판단해 여기 진행.

## 목표

**현재 gRPC 사이드카(Node ↔ Go) 방식의 실 처리 파이프라인에 Kafka 기반 분산 워커 백엔드를 추가한다.**

관측성·대시보드 데이터가 아닌 **실 처리 데이터**에 붙임. 이유: 카프카의 존재 이유(내구성·수평 확장)와 정확히 맞물리는 지점.

## 이미 확정된 의사결정 (이 세션에서 결론난 것)

### 1. 카프카 배포 방식 → **Docker Compose (로컬)** ✅
- Confluent Cloud 무료 티어 대신 로컬 컴포즈
- 이유: 개인 프로젝트 인프라 비용 $0 원칙 (전역 CLAUDE.md), 로컬 개발 편의성

### 2. Node 클라이언트 → **kafkajs 직접 사용** ✅ (확정)
- `@nestjs/microservices` Kafka transport 아님
- 이유:
  1. NestJS 데코레이터에 카프카 개념(파티션·오프셋·컨슈머 그룹) 감춰지면 학습 목적 훼손. 면접에서 "파티션 어떻게 다뤘어요?" 답 못 함
  2. 어댑터 패턴 규칙 준수 (전역 CLAUDE.md: 외부 의존성은 어댑터 인터페이스 뒤에 숨김) — 프레임워크가 벤더 종속시키는 방식보다 자연스러움
  3. `@nestjs/microservices`도 결국 내부에서 kafkajs를 씀. "공식이 안정적"이라는 관점은 근거 없음 (밑바닥 같음)
  4. 오프셋 수동 커밋·리밸런싱·트랜잭션 등 세밀 제어가 자유로움

### 3. Go 컨슈머 라이브러리 → **segmentio/kafka-go**
- confluent-kafka-go 아님 (C 라이브러리 의존)
- 이유: 순수 Go, 크로스컴파일 편함, Go 관용어법

### 4. 카프카 이미지 → **bitnami/kafka (KRaft 모드)**
- Zookeeper 없이 단독 실행
- 환경변수 이름이 카프카 원래 설정 이름과 비슷 → 학습 좋음

### 5. 우선순위 처리 방식 → **토픽 3개 분리** (`tasks.high` / `tasks.normal` / `tasks.low`)
- 파티션을 우선순위로 쓰는 방식 아님 (파티션 원 목적은 병렬 분산)
- 카프카 관용어법에 맞음

## 아키텍처 (붙인 후)

```
Client → NestJS Producer ──> Kafka Topic (tasks.high / normal / low)
                                    ↓ ↓ ↓
                          ┌─────────┼─────────┐
                          ↓         ↓         ↓
                    Go Consumer  Go Consumer  Go Consumer
                    (기존 pool/router 재활용)
```

**얻는 것**:
- Go 컨슈머 죽어도 → 컨슈머 그룹 리밸런싱으로 나머지가 인수
- Node 재시작해도 → 카프카에 메시지 남아있음 (내구성)
- 컨슈머 늘리고 싶으면 → Go 프로세스 추가로 자동 부하 분산
- gRPC in-flight 요청 손실 문제 사라짐 (브로커가 중간 버퍼)

## 회고 문서와의 연결

`docs/회고.md:127` "향후 결정 대기 — 분산 워커 (현재 단일 노드)"를 이 작업이 해결한다. **뒤늦게 이유 만들어 붙인 게 아니라, 원래 남겨둔 숙제를 이제 푸는 것**.

## 구현 순서

1. **docker-compose.yml에 kafka + kafka-ui 서비스 추가** (KRaft 모드, 9092 외부 노출, kafka-ui 8090)
   - ⚠️ 진행 중이었음: `bitnami/kafka:3.7` 태그가 hub에서 사라져서 태그 확인 필요. `bitnami/kafka:latest` 또는 `confluentinc/cp-kafka:latest`로 교체 결정 필요
2. **NestJS 어댑터 인터페이스 `WorkerBackend` 정의**
   - 기존 `WorkerPoolService` → `NodeWorkerBackend`로 래핑
   - 기존 `GoEngineClient` (gRPC) → `GrpcGoBackend`로 래핑
   - 신규 `KafkaBackend` (kafkajs 프로듀서)
3. **env 스위치 `WORKER_ENGINE=node|go|both|kafka` 확장**
4. **Go 컨슈머 추가** `go-engine/cmd/consumer/main.go` 신규
   - segmentio/kafka-go로 3개 토픽 (high/normal/low) 소비
   - 기존 `internal/pool` / `internal/router` / `internal/worker` 재활용 (핸들러는 그대로, 진입점만 gRPC 서버 → kafka consumer)
   - `WORKER_ENGINE=kafka`용 새 컨테이너 추가 (`go-consumer` 서비스)
5. **벤치마크 재실행** (기존 `benchmark/` 폴더 사용)
   - before: gRPC 동기 (throughput, p99, 재시작 손실률)
   - after: 카프카 비동기 (throughput, p99, 재시작 손실률)
   - 목표 지표: throughput 증가, 재시작 손실률 0% 달성
6. **회고 문서 업데이트** (`docs/회고.md` "분산 워커" 항목을 실현 완료로 이동, 새 콘텐츠 후보 "gRPC 사이드카에서 Kafka 분산 워커로" 추가)
7. **오답노트 기록** (`~/Desktop/private_repo/오답노트/`): Node 24 업그레이드 후 better-sqlite3 재빌드 이슈 (이번 스모크에서 발견됨)

## 이번 세션에서 실제로 한 일

- [x] 프로젝트 파악 (README, 회고, go-heavy-worker-engine 설계 문서)
- [x] Go 엔진 실제 완성도 확인: 1,519줄, 테스트 포함, 바이너리 컴파일됨. **미완이 아니라 잘 완성됨**
- [x] 스모크 확인: `npm run build` OK, `npm test` 77개 통과, 서버 기동 OK (Node 24 재빌드 후)
- [x] better-sqlite3 재빌드 필요 이슈 발견 → `npm rebuild better-sqlite3`로 해결
- [x] docker-compose.yml에 kafka + kafka-ui 서비스 추가 (KRaft 모드)
- [x] volumes에 kafka-data 추가
- [ ] **미완**: `bitnami/kafka:3.7` 태그 hub에서 사라짐 → 태그 교체 필요 (`bitnami/kafka:latest` 검토 또는 `confluentinc/cp-kafka:latest`로 교체). 여기서 중단됨

## 다음 세션이 이어받을 때

**첫 액션**: docker-compose.yml에서 `bitnami/kafka:3.7` 라인 수정. 옵션 두 개:

**옵션 A**: `bitnami/kafka:latest` (현재 컴포즈 스타일 그대로 유지, 환경변수 변경 없이 재시도)
**옵션 B**: `confluentinc/cp-kafka:latest`로 교체 (환경변수 이름 다름 — `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES` 등. bitnami는 `KAFKA_CFG_` 접두사)

행님과 결정 후 진행.

**그 다음 액션**: `docker compose up -d kafka kafka-ui` → `docker compose logs kafka` 로 정상 기동 확인 → kafka-ui (http://localhost:8090) 접속해서 UI 살펴보기 → 그다음 kafkajs 어댑터 코드 시작.

## 카프카 개념 요약 (다음 세션 컨텍스트 절약용)

- **Topic**: 메시지 채널 (단톡방)
- **Producer**: 메시지 보내는 쪽 (Node)
- **Consumer**: 메시지 읽는 쪽 (Go)
- **Broker**: 카프카 서버 자체
- **Partition**: 토픽을 여러 조각으로 나눠 병렬화 (파티션 하나 = 컨슈머 하나 담당)
- **Offset**: 메시지마다 붙는 순서 번호. 컨슈머가 "나 여기까지 읽었어" 기록
- **Consumer Group**: 컨슈머 여러 명을 한 팀으로 묶음. 파티션 자동 분배·리밸런싱
- **KRaft 모드**: Zookeeper 없이 카프카 단독 실행 (2022+ 표준)
- **읽어도 메시지 안 지워짐** (설정 보관 기간 동안 유지) — Redis 큐와 결정적 차이

## 참고 문서

- 프로젝트 회고: `docs/회고.md`
- Go 엔진 설계: `docs/go-heavy-worker-engine.md`
- 이 계획 자체는 이력서·연봉 분석에서 도출됨:
  - `~/Documents/Obsidian Vault/career/kpi/2026-07-05-점핏연봉리포트.md` — Kafka 스택 평균 연봉 62,423,333원 (백엔드 스택 최상위권)
  - `~/Documents/Obsidian Vault/career/kpi/2026-07-05-실제스택자산-갭분석.md` — 원티드 시장 Kafka 수요 10.4% (행님 없는 스택 중 학습 ROI 1위)
