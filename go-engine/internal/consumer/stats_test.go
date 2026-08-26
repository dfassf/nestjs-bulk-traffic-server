package consumer

import (
	"sync"
	"testing"
	"time"
)

// 한 건도 처리 못 했는데 지연을 0 으로 내보내면 가장 빠른 결과처럼 읽힌다.
func TestSnapshotWithoutSamplesReportsNil(t *testing.T) {
	stats := NewStats("c-1")

	snapshot := stats.Snapshot()

	if snapshot.AvgLatency != nil {
		t.Errorf("표본이 없으면 평균은 nil 이어야 합니다: %v", *snapshot.AvgLatency)
	}
	if snapshot.P50Latency != nil || snapshot.P95Latency != nil {
		t.Error("표본이 없으면 분위수도 nil 이어야 합니다")
	}
	if snapshot.Throughput != nil {
		t.Errorf("처리 건이 없으면 처리량은 nil 이어야 합니다: %v", *snapshot.Throughput)
	}
}

// 전량 실패도 마찬가지다. 실패만 쌓였는데 평균 0ms 로 보이면 안 된다.
func TestSnapshotWithOnlyFailuresReportsNil(t *testing.T) {
	stats := NewStats("c-1")
	stats.RecordFailure()
	stats.RecordFailure()

	snapshot := stats.Snapshot()

	if snapshot.Failed != 2 {
		t.Errorf("실패 건수가 다릅니다: %d", snapshot.Failed)
	}
	if snapshot.AvgLatency != nil {
		t.Error("성공 건이 없으면 평균은 nil 이어야 합니다")
	}
}

// 처리량은 "일한 시간" 으로 나눠야 한다. 메시지를 기다리며 논 시간까지
// 분모에 넣으면, 처리가 끝난 뒤에도 숫자가 계속 나빠진다.
// 두 런타임을 비교할 때 먼저 뜬 쪽이 이유 없이 불리해진다.
func TestThroughputIgnoresIdleTime(t *testing.T) {
	stats := NewStats("c-1")

	// 컨슈머는 떴지만 한동안 아무것도 안 들어온 상황.
	time.Sleep(30 * time.Millisecond)

	// 그 뒤 두 건을 빠르게 처리했다.
	stats.RecordSuccess(0, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	stats.RecordSuccess(0, time.Millisecond)

	snapshot := stats.Snapshot()

	if snapshot.Throughput == nil {
		t.Fatal("두 건을 처리했으면 처리량이 나와야 합니다")
	}
	// 논 시간(30ms)까지 셌다면 2/0.04 = 50건/초 아래로 떨어진다.
	// 일한 시간(약 10ms)만 셌다면 2/0.01 = 약 200건/초.
	if *snapshot.Throughput < 100 {
		t.Errorf("논 시간이 처리량에 섞였습니다: %.1f건/초", *snapshot.Throughput)
	}
}

// 한 건뿐이면 잰 구간이 없다. 0 으로 메우면 "처리량 0" 으로 보인다.
func TestThroughputWithSingleSampleIsUnmeasured(t *testing.T) {
	stats := NewStats("c-1")
	stats.RecordSuccess(0, time.Millisecond)

	snapshot := stats.Snapshot()

	if snapshot.Throughput != nil {
		t.Errorf("한 건으로는 처리량을 잴 수 없습니다: %v", *snapshot.Throughput)
	}
	// 처리 건수 자체는 남아야 한다.
	if snapshot.Processed != 1 {
		t.Errorf("처리 건수가 다릅니다: %d", snapshot.Processed)
	}
}

func TestFormatDurationShowsUnmeasured(t *testing.T) {
	if got := FormatDuration(nil); got != "측정 불가" {
		t.Errorf("nil 은 '측정 불가' 여야 합니다: %s", got)
	}

	d := 5 * time.Millisecond
	if got := FormatDuration(&d); got != "5ms" {
		t.Errorf("값이 있으면 그대로 보여야 합니다: %s", got)
	}
}

func TestFormatThroughputShowsUnmeasured(t *testing.T) {
	if got := FormatThroughput(nil); got != "측정 불가" {
		t.Errorf("nil 은 '측정 불가' 여야 합니다: %s", got)
	}
}

func TestRecordSuccessCountsPerPartition(t *testing.T) {
	stats := NewStats("c-1")
	stats.RecordSuccess(0, time.Millisecond)
	stats.RecordSuccess(0, time.Millisecond)
	stats.RecordSuccess(3, time.Millisecond)

	snapshot := stats.Snapshot()

	if snapshot.Processed != 3 {
		t.Errorf("처리 건수가 다릅니다: %d", snapshot.Processed)
	}
	if snapshot.PartitionCounts[0] != 2 || snapshot.PartitionCounts[3] != 1 {
		t.Errorf("파티션 분포가 다릅니다: %v", snapshot.PartitionCounts)
	}
}

func TestPercentileOrdersSamples(t *testing.T) {
	stats := NewStats("c-1")
	// 일부러 뒤섞어 넣는다. 정렬하지 않으면 분위수가 틀린다.
	for _, ms := range []int{50, 10, 90, 20, 30} {
		stats.RecordSuccess(0, time.Duration(ms)*time.Millisecond)
	}

	snapshot := stats.Snapshot()

	if *snapshot.P50Latency != 30*time.Millisecond {
		t.Errorf("p50 이 다릅니다: %s", *snapshot.P50Latency)
	}
	if *snapshot.P95Latency != 90*time.Millisecond {
		t.Errorf("p95 가 다릅니다: %s", *snapshot.P95Latency)
	}
}

// 분위수를 내림으로 잡으면 느린 꼬리가 가려져 성능이 실제보다 좋아 보인다.
func TestPercentileDoesNotHideSlowTail(t *testing.T) {
	stats := NewStats("c-1")
	// 99건은 1ms, 1건만 500ms. p95 는 느린 건을 가려도 되지만
	// p100 성격의 최댓값까지 놓치면 안 된다.
	for i := 0; i < 99; i++ {
		stats.RecordSuccess(0, time.Millisecond)
	}
	stats.RecordSuccess(0, 500*time.Millisecond)

	snapshot := stats.Snapshot()

	// 100건 중 95번째는 여전히 1ms 다. 여기까지는 정상.
	if *snapshot.P95Latency != time.Millisecond {
		t.Errorf("p95 가 다릅니다: %s", *snapshot.P95Latency)
	}
	// 평균은 느린 건을 반영해야 한다.
	if *snapshot.AvgLatency <= time.Millisecond {
		t.Errorf("평균이 느린 건을 반영하지 않았습니다: %s", *snapshot.AvgLatency)
	}
}

func TestPercentileWithSingleSample(t *testing.T) {
	stats := NewStats("c-1")
	stats.RecordSuccess(0, 7*time.Millisecond)

	snapshot := stats.Snapshot()

	// 한 건뿐이면 어느 분위수든 그 값이다. 인덱스가 범위를 벗어나면 안 된다.
	if *snapshot.P50Latency != 7*time.Millisecond {
		t.Errorf("p50 이 다릅니다: %s", *snapshot.P50Latency)
	}
	if *snapshot.P95Latency != 7*time.Millisecond {
		t.Errorf("p95 가 다릅니다: %s", *snapshot.P95Latency)
	}
}

func TestSnapshotCopiesPartitionCounts(t *testing.T) {
	stats := NewStats("c-1")
	stats.RecordSuccess(0, time.Millisecond)

	snapshot := stats.Snapshot()
	snapshot.PartitionCounts[0] = 999

	// 떠 준 값을 바꿔도 원본이 흔들리면 안 된다.
	if again := stats.Snapshot(); again.PartitionCounts[0] != 1 {
		t.Errorf("스냅샷이 원본을 공유하고 있습니다: %d", again.PartitionCounts[0])
	}
}

// 여러 컨슈머가 동시에 집계하므로 경합에서 건수가 새면 안 된다.
// go test -race 로 돌리면 자료 경합 자체도 잡힌다.
func TestStatsIsSafeForConcurrentUse(t *testing.T) {
	stats := NewStats("c-1")
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats.RecordSuccess(1, time.Millisecond)
		}()
	}
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats.RecordFailure()
		}()
	}
	wg.Wait()

	snapshot := stats.Snapshot()
	if snapshot.Processed != 50 {
		t.Errorf("동시 집계에서 처리 건수가 샜습니다: %d", snapshot.Processed)
	}
	if snapshot.Failed != 30 {
		t.Errorf("동시 집계에서 실패 건수가 샜습니다: %d", snapshot.Failed)
	}
}
