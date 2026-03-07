package pool

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// SubmitSync enqueues a task and waits for the result.
func (p *Pool) SubmitSync(ctx context.Context, task *Task) TaskResult {
	if p.queue.QueueLen() >= p.config.MaxQueue {
		return TaskResult{
			TaskID:  task.ID,
			Success: false,
			Error:   fmt.Sprintf("pool %s: queue full", p.config.Name),
		}
	}

	start := time.Now()
	timeout := p.config.Timeout
	if task.TimeoutMs > 0 {
		timeout = time.Duration(task.TimeoutMs) * time.Millisecond
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if p.limiter != nil {
		if err := p.limiter.Wait(taskCtx); err != nil {
			return TaskResult{
				TaskID:  task.ID,
				Success: false,
				Error:   fmt.Sprintf("rate limit: %v", err),
			}
		}
	}

	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-taskCtx.Done():
		return TaskResult{
			TaskID:  task.ID,
			Success: false,
			Error:   "timeout waiting for worker",
		}
	}

	atomic.AddInt32(&p.stats.ActiveWorkers, 1)
	defer atomic.AddInt32(&p.stats.ActiveWorkers, -1)

	result, err := p.executeWithRetry(taskCtx, task, p.handler)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		atomic.AddInt64(&p.stats.Failed, 1)
		return TaskResult{
			TaskID:     task.ID,
			Success:    false,
			Error:      err.Error(),
			DurationMs: duration,
		}
	}

	atomic.AddInt64(&p.stats.Processed, 1)
	atomic.AddInt64(&p.stats.TotalLatency, duration)
	p.stats.recordLatency(duration)
	return TaskResult{
		TaskID:     task.ID,
		Success:    true,
		Result:     result,
		DurationMs: duration,
	}
}
