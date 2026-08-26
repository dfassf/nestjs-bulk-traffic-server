package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Stats 는 컨슈머 하나의 처리 현황이다.
//
// 여러 고루틴이 함께 건드리므로 잠금으로 보호한다. 성능을 재는 코드라
// 집계 자체가 병목이 되면 안 되므로, 잠금 구간은 값 하나 더하는 수준으로 짧게 둔다.
type Stats struct {
	mu sync.Mutex

	consumerID string
	processed  int
	failed     int

	// partitionCounts 는 파티션별 처리 건수다. 분배가 고른지 볼 때 쓴다.
	partitionCounts map[int]int

	// latencies 는 한 건을 처리하는 데 걸린 시간이다.
	// 평균만 보면 느린 꼬리가 안 보여서 분포를 남긴다.
	latencies []time.Duration

	// firstProcessed 는 첫 건을 처리한 시각이다.
	//
	// 컨슈머를 만든 시각이 아니다. 처리량은 "일한 시간" 으로 나눠야 하는데,
	// 메시지를 기다리며 논 시간까지 분모에 넣으면 가만히 있을수록 숫자가
	// 계속 나빠진다. 두 런타임을 비교할 때 먼저 뜬 쪽이 불리해진다.
	firstProcessed time.Time
	lastProcessed  time.Time
}

func NewStats(consumerID string) *Stats {
	return &Stats{
		consumerID:      consumerID,
		partitionCounts: make(map[int]int),
	}
}

func (s *Stats) RecordSuccess(partition int, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if s.processed == 0 {
		s.firstProcessed = now
	}

	s.processed++
	s.partitionCounts[partition]++
	s.latencies = append(s.latencies, latency)
	s.lastProcessed = now
}

func (s *Stats) RecordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failed++
}

func (s *Stats) Processed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processed
}

// Snapshot 은 지금까지의 집계를 값으로 떠 준다.
type Snapshot struct {
	ConsumerID      string
	Processed       int
	Failed          int
	PartitionCounts map[int]int
	Elapsed         time.Duration

	// 아래 지연 지표는 성공 건이 하나도 없으면 nil 이다.
	//
	// 0 으로 메우지 않는다. 지연 0ms 는 가장 좋아 보이는 값이라,
	// 한 건도 처리 못 한 결과가 가장 빠른 것처럼 읽힌다.
	AvgLatency *time.Duration
	P50Latency *time.Duration
	P95Latency *time.Duration

	// Throughput 은 초당 처리 건수다. 아직 시간이 안 흘렀으면 nil.
	Throughput *float64
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[int]int, len(s.partitionCounts))
	for partition, count := range s.partitionCounts {
		counts[partition] = count
	}

	// 첫 처리부터 마지막 처리까지가 실제로 일한 시간이다.
	elapsed := s.lastProcessed.Sub(s.firstProcessed)
	snapshot := Snapshot{
		ConsumerID:      s.consumerID,
		Processed:       s.processed,
		Failed:          s.failed,
		PartitionCounts: counts,
		Elapsed:         elapsed,
		AvgLatency:      average(s.latencies),
		P50Latency:      percentile(s.latencies, 50),
		P95Latency:      percentile(s.latencies, 95),
	}

	// 두 건 이상이라야 사이 간격이 생긴다. 한 건뿐이면 잰 구간이 0 이라
	// 나눌 수 없다. 그때는 0 으로 메우지 않고 측정 불가로 둔다.
	if s.processed > 1 && elapsed > 0 {
		perSec := float64(s.processed) / elapsed.Seconds()
		snapshot.Throughput = &perSec
	}

	return snapshot
}

func average(values []time.Duration) *time.Duration {
	if len(values) == 0 {
		return nil
	}

	var total time.Duration
	for _, value := range values {
		total += value
	}
	avg := total / time.Duration(len(values))
	return &avg
}

// percentile 은 정렬한 뒤 해당 위치의 값을 고른다.
// 표본이 없으면 nil 이다(0 이 아니다).
//
// 위치는 올림으로 잡는다. 내림으로 잡으면 p95 가 실제보다 낮게 나와
// 느린 꼬리가 가려지고, 성능이 실제보다 좋아 보인다.
// 예: 10·20·30·50·90ms 다섯 건에서 내림은 50ms, 올림은 90ms 를 준다.
func percentile(values []time.Duration, p int) *time.Duration {
	if len(values) == 0 {
		return nil
	}

	sorted := make([]time.Duration, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	// 올림 나눗셈. 나머지가 있으면 한 칸 위를 본다.
	index := (len(sorted)*p + 99) / 100
	if index > 0 {
		index--
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return &sorted[index]
}

// FormatDuration 은 측정 못 한 값을 "측정 불가" 로 보여준다.
// 0ms 로 찍으면 가장 빠른 결과처럼 읽힌다.
func FormatDuration(d *time.Duration) string {
	if d == nil {
		return "측정 불가"
	}
	return d.String()
}

// FormatThroughput 은 초당 처리 건수를 보여준다.
func FormatThroughput(v *float64) string {
	if v == nil {
		return "측정 불가"
	}
	return fmt.Sprintf("%.1f건/초", *v)
}
