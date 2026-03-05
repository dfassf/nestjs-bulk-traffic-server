package server

import (
	"context"
	"log"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/metrics"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/router"
	pb "github.com/dfassf/nestjs-bulk-traffic-server/go-engine/proto"
)

var startTime = time.Now()
var version = "1.0.0"

// GRPCServer implements the WorkerEngine gRPC service.
type GRPCServer struct {
	pb.UnimplementedWorkerEngineServer
	router *router.Router
}

// NewGRPCServer creates a new gRPC server.
func NewGRPCServer(r *router.Router) *GRPCServer {
	return &GRPCServer{router: r}
}

// Execute handles a task execution request.
func (s *GRPCServer) Execute(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("[gRPC] Execute task=%s type=%s priority=%d", req.TaskId, req.WorkloadType, req.Priority)

	task := &pool.Task{
		ID:           req.TaskId,
		WorkloadType: req.WorkloadType,
		Priority:     req.Priority,
		Payload:      req.Payload,
		TimeoutMs:    req.TimeoutMs,
		Metadata:     req.Metadata,
	}

	result := s.router.Execute(ctx, task)

	// Record metrics
	status := "success"
	if !result.Success {
		status = "failed"
	}

	poolName := req.WorkloadType
	if poolName == "memory" || poolName == "custom" || poolName == "" {
		poolName = "cpu"
	}

	metrics.TasksProcessed.WithLabelValues(poolName, status).Inc()
	metrics.TaskDuration.WithLabelValues(poolName).Observe(float64(result.DurationMs))

	return &pb.TaskResponse{
		TaskId:     result.TaskID,
		Success:    result.Success,
		Result:     result.Result,
		Error:      result.Error,
		DurationMs: result.DurationMs,
		Engine:     "go",
	}, nil
}

// GetStats returns engine statistics.
func (s *GRPCServer) GetStats(ctx context.Context, _ *pb.Empty) (*pb.EngineStats, error) {
	var totalProcessed, totalFailed, activeTasks int64
	poolStats := make(map[string]*pb.PoolStats)

	for name, p := range s.router.GetPools() {
		stats := p.GetStats()
		totalProcessed += stats.Processed
		totalFailed += stats.Failed
		activeTasks += int64(stats.ActiveWorkers)

		poolStats[name] = &pb.PoolStats{
			ActiveWorkers: stats.ActiveWorkers,
			QueueLength:   stats.QueueLength,
			Processed:     stats.Processed,
			Failed:        stats.Failed,
			AvgLatencyMs:  stats.AvgLatencyMs(),
			P99LatencyMs:  stats.P99LatencyMs(),
		}

		// Update Prometheus gauges
		metrics.ActiveWorkers.WithLabelValues(name).Set(float64(stats.ActiveWorkers))
		metrics.QueueLength.WithLabelValues(name).Set(float64(stats.QueueLength))
	}

	return &pb.EngineStats{
		TotalProcessed: totalProcessed,
		TotalFailed:    totalFailed,
		ActiveTasks:    activeTasks,
		Pools:          poolStats,
	}, nil
}

// HealthCheck returns health status.
func (s *GRPCServer) HealthCheck(ctx context.Context, _ *pb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy:       true,
		Version:       version,
		UptimeSeconds: int64(time.Since(startTime).Seconds()),
	}, nil
}
