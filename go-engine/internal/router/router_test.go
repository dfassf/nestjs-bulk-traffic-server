package router

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

func testHandler(ctx context.Context, task *pool.Task) ([]byte, error) {
	return json.Marshal(map[string]string{"task_id": task.ID, "type": task.WorkloadType})
}

func setupRouter() *Router {
	cpuPool := pool.NewPool(pool.PoolConfig{
		Name:     "cpu",
		Workers:  2,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    pool.RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, testHandler)

	ioPool := pool.NewPool(pool.PoolConfig{
		Name:     "io",
		Workers:  2,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    pool.RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, testHandler)

	batchPool := pool.NewPool(pool.PoolConfig{
		Name:     "batch",
		Workers:  2,
		MaxQueue: 100,
		Timeout:  5 * time.Second,
		Retry:    pool.RetryConfig{MaxRetries: 0, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}, testHandler)

	cpuPool.Start()
	ioPool.Start()
	batchPool.Start()

	return NewRouter(map[string]*pool.Pool{
		"cpu":   cpuPool,
		"io":    ioPool,
		"batch": batchPool,
	})
}

func TestRouter_RouteCPU(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("cpu")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "cpu" {
		t.Errorf("expected cpu pool, got %s", p.Name())
	}
}

func TestRouter_RouteIO(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("io")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "io" {
		t.Errorf("expected io pool, got %s", p.Name())
	}
}

func TestRouter_RouteBatch(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("batch")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "batch" {
		t.Errorf("expected batch pool, got %s", p.Name())
	}
}

func TestRouter_RouteMemoryFallback(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("memory")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "cpu" {
		t.Errorf("expected cpu pool for memory fallback, got %s", p.Name())
	}
}

func TestRouter_RouteCustomFallback(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("custom")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "cpu" {
		t.Errorf("expected cpu pool for custom fallback, got %s", p.Name())
	}
}

func TestRouter_RouteUnknownFallback(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	p, err := r.Route("something_else")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "cpu" {
		t.Errorf("expected cpu pool for unknown fallback, got %s", p.Name())
	}
}

func TestRouter_Execute(t *testing.T) {
	r := setupRouter()
	defer r.Stop()

	result := r.Execute(context.Background(), &pool.Task{
		ID:           "exec-1",
		WorkloadType: "cpu",
		Priority:     5,
		Payload:      []byte(`{}`),
	})

	if !result.Success {
		t.Fatalf("expected success, got error: %s", result.Error)
	}
	if result.TaskID != "exec-1" {
		t.Errorf("expected task_id exec-1, got %s", result.TaskID)
	}
}
