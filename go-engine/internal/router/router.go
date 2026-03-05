package router

import (
	"context"
	"fmt"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

// Router distributes tasks to appropriate pools based on workload type.
type Router struct {
	pools map[string]*pool.Pool
}

// NewRouter creates a router with the given pool mapping.
func NewRouter(pools map[string]*pool.Pool) *Router {
	return &Router{pools: pools}
}

// Route selects the appropriate pool for a workload type.
func (r *Router) Route(workloadType string) (*pool.Pool, error) {
	switch workloadType {
	case "cpu":
		return r.getPool("cpu")
	case "io":
		return r.getPool("io")
	case "batch":
		return r.getPool("batch")
	case "memory", "custom":
		return r.getPool("cpu") // fallback to CPU pool
	default:
		return r.getPool("cpu") // default fallback
	}
}

// Execute routes a task to the appropriate pool and returns the result synchronously.
func (r *Router) Execute(ctx context.Context, task *pool.Task) pool.TaskResult {
	p, err := r.Route(task.WorkloadType)
	if err != nil {
		return pool.TaskResult{
			TaskID:  task.ID,
			Success: false,
			Error:   err.Error(),
		}
	}
	return p.SubmitSync(ctx, task)
}

func (r *Router) getPool(name string) (*pool.Pool, error) {
	p, ok := r.pools[name]
	if !ok {
		return nil, fmt.Errorf("pool %q not found", name)
	}
	return p, nil
}

// GetPools returns all managed pools.
func (r *Router) GetPools() map[string]*pool.Pool {
	return r.pools
}

// Stop shuts down all pools.
func (r *Router) Stop() {
	for _, p := range r.pools {
		p.Stop()
	}
}
