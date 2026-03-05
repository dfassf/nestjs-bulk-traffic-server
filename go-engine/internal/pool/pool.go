package pool

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// TaskResult holds the outcome of a processed task.
type TaskResult struct {
	TaskID     string
	Success    bool
	Result     []byte
	Error      string
	DurationMs int64
}

// Handler processes a task payload and returns a result.
type Handler func(ctx context.Context, task *Task) ([]byte, error)

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
	// Keep only last 1000 for P99
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
	// Simple sorted approach for p99
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
	// insertion sort is fine for <=1000 elements
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

// Pool manages a set of goroutine workers with a priority queue.
type Pool struct {
	config  PoolConfig
	queue   *PriorityQueue
	handler Handler
	limiter *rate.Limiter
	sem     chan struct{} // semaphore for worker count
	stats   Stats
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewPool creates a new worker pool.
func NewPool(cfg PoolConfig, handler Handler) *Pool {
	p := &Pool{
		config:  cfg,
		queue:   NewPriorityQueue(),
		handler: handler,
		sem:     make(chan struct{}, cfg.Workers),
		stopCh:  make(chan struct{}),
	}
	if cfg.RateLimit > 0 {
		p.limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit), int(cfg.RateLimit))
	}
	return p
}

// Start launches the pool's dispatch loop.
func (p *Pool) Start() {
	p.wg.Add(1)
	go p.dispatchLoop()
}

// Stop gracefully shuts down the pool.
func (p *Pool) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// Submit enqueues a task. Returns error if queue is full.
func (p *Pool) Submit(task *Task) error {
	if p.queue.QueueLen() >= p.config.MaxQueue {
		return fmt.Errorf("pool %s: queue full (%d/%d)", p.config.Name, p.queue.QueueLen(), p.config.MaxQueue)
	}
	p.queue.Enqueue(task)
	return nil
}

// SubmitSync enqueues a task and waits for the result.
func (p *Pool) SubmitSync(ctx context.Context, task *Task) TaskResult {
	ch := make(chan TaskResult, 1)

	wrappedHandler := p.handler
	originalHandler := p.handler

	// Temporarily use a handler that sends result to channel
	taskCopy := *task
	go func() {
		if p.queue.QueueLen() >= p.config.MaxQueue {
			ch <- TaskResult{
				TaskID:  task.ID,
				Success: false,
				Error:   fmt.Sprintf("pool %s: queue full", p.config.Name),
			}
			return
		}

		start := time.Now()

		timeout := p.config.Timeout
		if task.TimeoutMs > 0 {
			timeout = time.Duration(task.TimeoutMs) * time.Millisecond
		}

		taskCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Rate limit
		if p.limiter != nil {
			if err := p.limiter.Wait(taskCtx); err != nil {
				ch <- TaskResult{
					TaskID:  task.ID,
					Success: false,
					Error:   fmt.Sprintf("rate limit: %v", err),
				}
				return
			}
		}

		// Acquire semaphore
		select {
		case p.sem <- struct{}{}:
			defer func() { <-p.sem }()
		case <-taskCtx.Done():
			ch <- TaskResult{
				TaskID:  task.ID,
				Success: false,
				Error:   "timeout waiting for worker",
			}
			return
		}

		atomic.AddInt32(&p.stats.ActiveWorkers, 1)
		defer atomic.AddInt32(&p.stats.ActiveWorkers, -1)

		_ = wrappedHandler
		result, err := p.executeWithRetry(taskCtx, &taskCopy, originalHandler)
		duration := time.Since(start).Milliseconds()

		if err != nil {
			atomic.AddInt64(&p.stats.Failed, 1)
			ch <- TaskResult{
				TaskID:     task.ID,
				Success:    false,
				Error:      err.Error(),
				DurationMs: duration,
			}
		} else {
			atomic.AddInt64(&p.stats.Processed, 1)
			atomic.AddInt64(&p.stats.TotalLatency, duration)
			p.stats.recordLatency(duration)
			ch <- TaskResult{
				TaskID:     task.ID,
				Success:    true,
				Result:     result,
				DurationMs: duration,
			}
		}
	}()

	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return TaskResult{
			TaskID:  task.ID,
			Success: false,
			Error:   "context cancelled",
		}
	}
}

func (p *Pool) dispatchLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			task := p.queue.Dequeue()
			if task == nil {
				continue
			}
			p.dispatch(task)
		}
	}
}

func (p *Pool) dispatch(task *Task) {
	// Rate limit
	if p.limiter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.limiter.Wait(ctx)
		cancel()
	}

	// Acquire semaphore
	p.sem <- struct{}{}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()

		atomic.AddInt32(&p.stats.ActiveWorkers, 1)
		defer atomic.AddInt32(&p.stats.ActiveWorkers, -1)

		timeout := p.config.Timeout
		if task.TimeoutMs > 0 {
			timeout = time.Duration(task.TimeoutMs) * time.Millisecond
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		start := time.Now()
		_, err := p.executeWithRetry(ctx, task, p.handler)
		duration := time.Since(start).Milliseconds()

		if err != nil {
			atomic.AddInt64(&p.stats.Failed, 1)
		} else {
			atomic.AddInt64(&p.stats.Processed, 1)
			atomic.AddInt64(&p.stats.TotalLatency, duration)
			p.stats.recordLatency(duration)
		}
	}()
}

func (p *Pool) executeWithRetry(ctx context.Context, task *Task, handler Handler) ([]byte, error) {
	var lastErr error
	maxRetries := p.config.Retry.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := handler(ctx, task)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if attempt < maxRetries {
			delay := p.config.Retry.BaseDelay * time.Duration(1<<uint(attempt))
			if delay > p.config.Retry.MaxDelay && p.config.Retry.MaxDelay > 0 {
				delay = p.config.Retry.MaxDelay
			}
			// Add jitter
			jitter := time.Duration(rand.Int63n(int64(delay) / 2))
			delay = delay + jitter

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return nil, lastErr
}

// GetStats returns a snapshot of pool statistics.
func (p *Pool) GetStats() Stats {
	return Stats{
		ActiveWorkers: atomic.LoadInt32(&p.stats.ActiveWorkers),
		QueueLength:   int32(p.queue.QueueLen()),
		Processed:     atomic.LoadInt64(&p.stats.Processed),
		Failed:        atomic.LoadInt64(&p.stats.Failed),
		TotalLatency:  atomic.LoadInt64(&p.stats.TotalLatency),
	}
}

// Name returns the pool name.
func (p *Pool) Name() string {
	return p.config.Name
}
