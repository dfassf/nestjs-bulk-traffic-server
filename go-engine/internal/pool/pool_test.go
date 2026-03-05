package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func echoHandler(ctx context.Context, task *Task) ([]byte, error) {
	return json.Marshal(map[string]string{"task_id": task.ID})
}

func slowHandler(ctx context.Context, task *Task) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(200 * time.Millisecond):
		return []byte("done"), nil
	}
}

func failHandler(ctx context.Context, task *Task) ([]byte, error) {
	return nil, fmt.Errorf("intentional failure")
}

func TestPool_SubmitSync_Success(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  4,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, echoHandler)
	p.Start()
	defer p.Stop()

	result := p.SubmitSync(context.Background(), &Task{ID: "t1", Priority: 1})
	if !result.Success {
		t.Fatalf("expected success, got error: %s", result.Error)
	}
	if result.TaskID != "t1" {
		t.Errorf("expected task_id t1, got %s", result.TaskID)
	}
}

func TestPool_SubmitSync_Timeout(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  1,
		MaxQueue: 100,
		Timeout:  50 * time.Millisecond,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, slowHandler)
	p.Start()
	defer p.Stop()

	result := p.SubmitSync(context.Background(), &Task{ID: "t1", Priority: 1})
	if result.Success {
		t.Fatal("expected failure due to timeout")
	}
}

func TestPool_SubmitSync_Failure(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  2,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, failHandler)
	p.Start()
	defer p.Stop()

	result := p.SubmitSync(context.Background(), &Task{ID: "t1", Priority: 1})
	if result.Success {
		t.Fatal("expected failure")
	}
	if result.Error == "" {
		t.Error("expected error message")
	}
}

func TestPool_ConcurrentSubmit(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  8,
		MaxQueue: 1000,
		Timeout:  5 * time.Second,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, echoHandler)
	p.Start()
	defer p.Stop()

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result := p.SubmitSync(context.Background(), &Task{
				ID:       fmt.Sprintf("task-%d", id),
				Priority: int32(id % 10),
			})
			if result.Success {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	if successCount != 50 {
		t.Errorf("expected 50 successes, got %d", successCount)
	}
}

func TestPool_Stats(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  4,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, echoHandler)
	p.Start()
	defer p.Stop()

	p.SubmitSync(context.Background(), &Task{ID: "s1", Priority: 1})
	p.SubmitSync(context.Background(), &Task{ID: "s2", Priority: 1})

	stats := p.GetStats()
	if stats.Processed != 2 {
		t.Errorf("expected 2 processed, got %d", stats.Processed)
	}
	if stats.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.Failed)
	}
}

func TestPool_QueueFull(t *testing.T) {
	p := NewPool(PoolConfig{
		Name:     "test",
		Workers:  1,
		MaxQueue: 1,
		Timeout:  5 * time.Second,
		Retry:    RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, slowHandler)
	p.Start()
	defer p.Stop()

	// Fill the queue
	err := p.Submit(&Task{ID: "fill", Priority: 1})
	if err != nil {
		t.Fatalf("first submit should succeed: %v", err)
	}

	// Queue should be full now
	err = p.Submit(&Task{ID: "overflow", Priority: 1})
	if err == nil {
		t.Error("expected queue full error")
	}
}
