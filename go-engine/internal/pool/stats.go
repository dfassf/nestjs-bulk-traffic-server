package pool

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// TaskResult holds the outcome of a processed task.
type TaskResult struct {
	TaskID     string
	Success    bool
	Result     []byte
	Error      string
	DurationMs int64
}

// RetryConfig controls retry behavior.
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// PoolConfig configures a worker pool.
type PoolConfig struct {
	Name      string
	Workers   int
	MaxQueue  int
	RateLimit float64 // requests per second, 0 = unlimited
	Timeout   time.Duration
	Retry     RetryConfig
}

// Stats holds runtime statistics for a pool.
type Stats struct {
	ActiveWorkers int32
	QueueLength   int32
	Processed     int64
	Failed        int64
	TotalLatency  int64 // sum of all durations in ms (for avg calculation)
	Latencies     []int64
	mu            sync.Mutex
}

func (s *Stats) recordLatency(ms int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Latencies = append(s.Latencies, ms)
	if len(s.Latencies) > 1000 {
		s.Latencies = s.Latencies[len(s.Latencies)-1000:]
	}
}

func (s *Stats) AvgLatencyMs() float64 {
	total := atomic.LoadInt64(&s.TotalLatency)
	processed := atomic.LoadInt64(&s.Processed)
	if processed == 0 {
		return 0
	}
	return float64(total) / float64(processed)
}

func (s *Stats) P99LatencyMs() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.Latencies)
	if n == 0 {
		return 0
	}
	sorted := make([]int64, n)
	copy(sorted, s.Latencies)
	sortInt64s(sorted)
	idx := int(math.Ceil(float64(n)*0.99)) - 1
	if idx >= n {
		idx = n - 1
	}
	return float64(sorted[idx])
}

func sortInt64s(a []int64) {
	for i := 1; i < len(a); i++ {
		key := a[i]
		j := i - 1
		for j >= 0 && a[j] > key {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = key
	}
}
