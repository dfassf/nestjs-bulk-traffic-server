package pool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Handler processes a task payload and returns a result.
type Handler func(ctx context.Context, task *Task) ([]byte, error)

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
	if p.limiter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.limiter.Wait(ctx)
		cancel()
	}

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
