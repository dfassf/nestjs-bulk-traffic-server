#!/bin/bash
# 부하 테스트 스크립트
# 사용법: ./benchmark/load-test.sh [BASE_URL] [MODE]
# MODE: quick | normal | heavy

BASE_URL="${1:-http://localhost:3000}"
MODE="${2:-normal}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

header() { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }
ok() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; }
info() { echo -e "${YELLOW}→${NC} $1"; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time*1000'; }

# 서버 상태 확인
header "서버 상태 확인"
PING=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/load-test/ping" 2>/dev/null)
if [ "$PING" != "200" ]; then
  fail "서버 응답 없음 ($BASE_URL). 서버를 먼저 실행해주세요."
  echo "  npm run start:dev"
  exit 1
fi
ok "서버 정상 ($BASE_URL)"

ENGINE=$(curl -s "$BASE_URL/load-test/ping" | grep -o '"engine":"[^"]*"' | cut -d'"' -f4)
info "엔진 모드: $ENGINE"

# 설정
case "$MODE" in
  quick)
    CONCURRENT=5
    REQUESTS=20
    MIXED_COUNT=10
    ;;
  normal)
    CONCURRENT=10
    REQUESTS=50
    MIXED_COUNT=30
    ;;
  heavy)
    CONCURRENT=30
    REQUESTS=200
    MIXED_COUNT=100
    ;;
  *)
    info "알 수 없는 모드: $MODE (quick/normal/heavy)"
    exit 1
    ;;
esac

info "모드: $MODE (동시=$CONCURRENT, 요청=$REQUESTS, 혼합=$MIXED_COUNT)"

# 1. CPU 단일 요청
header "1. CPU 단일 요청"
START=$(now_ms)
RESULT=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/load-test/cpu" \
  -H "Content-Type: application/json" \
  -d '{"iterations": 5000}')
HTTP_CODE=$(echo "$RESULT" | tail -1)
END=$(now_ms)
ELAPSED=$((END - START))

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  ok "CPU 작업 완료 (${ELAPSED}ms)"
else
  fail "CPU 작업 실패 (HTTP $HTTP_CODE, ${ELAPSED}ms)"
fi

# 2. I/O 단일 요청
header "2. I/O 단일 요청"
START=$(now_ms)
RESULT=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/load-test/io" \
  -H "Content-Type: application/json" \
  -d '{"delayMs": 50}')
HTTP_CODE=$(echo "$RESULT" | tail -1)
END=$(now_ms)
ELAPSED=$((END - START))

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  ok "I/O 작업 완료 (${ELAPSED}ms)"
else
  fail "I/O 작업 실패 (HTTP $HTTP_CODE, ${ELAPSED}ms)"
fi

# 3. Batch 단일 요청
header "3. Batch 단일 요청"
START=$(now_ms)
RESULT=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/load-test/batch" \
  -H "Content-Type: application/json" \
  -d '{"itemCount": 5}')
HTTP_CODE=$(echo "$RESULT" | tail -1)
END=$(now_ms)
ELAPSED=$((END - START))

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  ok "Batch 작업 완료 (${ELAPSED}ms)"
else
  fail "Batch 작업 실패 (HTTP $HTTP_CODE, ${ELAPSED}ms)"
fi

# 4. 동시 CPU 부하
header "4. 동시 CPU 부하 (${CONCURRENT}개 동시)"
START=$(now_ms)
SUCCESS=0
FAIL=0
PIDS=()

for i in $(seq 1 $CONCURRENT); do
  (curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/load-test/cpu" \
    -H "Content-Type: application/json" \
    -d "{\"iterations\": 3000, \"priority\": $((RANDOM % 10 - 3))}") &
  PIDS+=($!)
done

for PID in "${PIDS[@]}"; do
  wait $PID
  CODE=$?
  if [ $CODE -eq 0 ]; then
    SUCCESS=$((SUCCESS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
done
END=$(now_ms)
ELAPSED=$((END - START))

ok "완료: 성공=$SUCCESS 실패=$FAIL (총 ${ELAPSED}ms, 평균 $((ELAPSED / CONCURRENT))ms)"

# 5. 동시 I/O 부하
header "5. 동시 I/O 부하 (${CONCURRENT}개 동시)"
START=$(now_ms)
PIDS=()

for i in $(seq 1 $CONCURRENT); do
  (curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/load-test/io" \
    -H "Content-Type: application/json" \
    -d "{\"delayMs\": 30}") &
  PIDS+=($!)
done

SUCCESS=0
for PID in "${PIDS[@]}"; do
  wait $PID && SUCCESS=$((SUCCESS + 1))
done
END=$(now_ms)
ELAPSED=$((END - START))

ok "완료: 성공=$SUCCESS/$CONCURRENT (총 ${ELAPSED}ms)"

# 6. 혼합 부하 (서버 측 동시 dispatch)
header "6. 혼합 부하 (${MIXED_COUNT}개 작업, 서버 측 dispatch)"
START=$(now_ms)
RESULT=$(curl -s -X POST "$BASE_URL/load-test/mixed" \
  -H "Content-Type: application/json" \
  -d "{\"count\": $MIXED_COUNT, \"cpuRatio\": 0.4, \"ioRatio\": 0.4}")
END=$(now_ms)
ELAPSED=$((END - START))

FULFILLED=$(echo "$RESULT" | grep -o '"fulfilled":[0-9]*' | cut -d: -f2)
REJECTED=$(echo "$RESULT" | grep -o '"rejected":[0-9]*' | cut -d: -f2)

if [ -n "$FULFILLED" ]; then
  ok "완료: 성공=$FULFILLED 거부=$REJECTED (${ELAPSED}ms)"
else
  fail "혼합 부하 실패 (${ELAPSED}ms)"
  echo "  $RESULT"
fi

# 7. 연속 부하 (순차)
header "7. 연속 부하 ($REQUESTS개 순차 요청)"
START=$(now_ms)
SEQ_SUCCESS=0
SEQ_FAIL=0

for i in $(seq 1 $REQUESTS); do
  TYPE=$((RANDOM % 3))
  case $TYPE in
    0) ENDPOINT="cpu"; DATA='{"iterations":1000}' ;;
    1) ENDPOINT="io"; DATA='{"delayMs":10}' ;;
    2) ENDPOINT="batch"; DATA='{"itemCount":3}' ;;
  esac

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/load-test/$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "$DATA")

  if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
    SEQ_SUCCESS=$((SEQ_SUCCESS + 1))
  else
    SEQ_FAIL=$((SEQ_FAIL + 1))
  fi
done
END=$(now_ms)
ELAPSED=$((END - START))

THROUGHPUT=$(echo "scale=1; $REQUESTS * 1000 / $ELAPSED" | bc 2>/dev/null || echo "N/A")
ok "완료: 성공=$SEQ_SUCCESS 실패=$SEQ_FAIL (${ELAPSED}ms, ${THROUGHPUT} req/s)"

# 8. 큐 상태 확인
header "8. 최종 큐 상태"
STATS=$(curl -s "$BASE_URL/queue-stats")
PROCESSED=$(echo "$STATS" | grep -o '"totalProcessed":[0-9]*' | cut -d: -f2)
REJECTED_Q=$(echo "$STATS" | grep -o '"totalRejected":[0-9]*' | cut -d: -f2)
TIMEOUT_Q=$(echo "$STATS" | grep -o '"totalTimeout":[0-9]*' | cut -d: -f2)
ACTIVE=$(echo "$STATS" | grep -o '"activeRequests":[0-9]*' | cut -d: -f2)
QUEUE_LEN=$(echo "$STATS" | grep -o '"totalQueueLength":[0-9]*' | cut -d: -f2)

info "처리: $PROCESSED | 거부: $REJECTED_Q | 타임아웃: $TIMEOUT_Q"
info "활성: $ACTIVE | 큐 잔여: $QUEUE_LEN"

# 벤치마크 (both 모드일 때)
if [ "$ENGINE" = "both" ]; then
  header "9. 벤치마크 결과"
  BENCH=$(curl -s "$BASE_URL/benchmark-stats")
  echo "$BENCH" | python3 -m json.tool 2>/dev/null || echo "$BENCH"
fi

header "테스트 완료"
