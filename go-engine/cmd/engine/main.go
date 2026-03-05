package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/metrics"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/router"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/server"
	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/worker"
	pb "github.com/dfassf/nestjs-bulk-traffic-server/go-engine/proto"
)

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	port := envInt("GO_ENGINE_PORT", 50051)
	metricsPort := envInt("GO_METRICS_PORT", 9090)

	poolAWorkers := envInt("GO_POOL_A_WORKERS", runtime.NumCPU())
	poolBWorkers := envInt("GO_POOL_B_WORKERS", 100)
	poolCWorkers := envInt("GO_POOL_C_WORKERS", 10)
	poolBRateLimit := envInt("GO_POOL_B_RATE_LIMIT", 100)
	poolCRateLimit := envInt("GO_POOL_C_RATE_LIMIT", 10)

	// Pool A: CPU-intensive
	cpuPool := pool.NewPool(pool.PoolConfig{
		Name:    "cpu",
		Workers: poolAWorkers,
		MaxQueue: 1000,
		Timeout: 30 * time.Second,
		Retry: pool.RetryConfig{
			MaxRetries: 3,
			BaseDelay:  100 * time.Millisecond,
			MaxDelay:   2 * time.Second,
		},
	}, worker.CPUHandler)

	// Pool B: I/O-intensive
	ioPool := pool.NewPool(pool.PoolConfig{
		Name:      "io",
		Workers:   poolBWorkers,
		MaxQueue:  5000,
		RateLimit: float64(poolBRateLimit),
		Timeout:   60 * time.Second,
		Retry: pool.RetryConfig{
			MaxRetries: 3,
			BaseDelay:  200 * time.Millisecond,
			MaxDelay:   5 * time.Second,
		},
	}, worker.IOHandler)

	// Pool C: Batch processing
	batchPool := pool.NewPool(pool.PoolConfig{
		Name:      "batch",
		Workers:   poolCWorkers,
		MaxQueue:  500,
		RateLimit: float64(poolCRateLimit),
		Timeout:   300 * time.Second,
		Retry: pool.RetryConfig{
			MaxRetries: 3,
			BaseDelay:  500 * time.Millisecond,
			MaxDelay:   10 * time.Second,
		},
	}, worker.BatchHandler)

	// Start pools
	cpuPool.Start()
	ioPool.Start()
	batchPool.Start()

	// Create router
	r := router.NewRouter(map[string]*pool.Pool{
		"cpu":   cpuPool,
		"io":    ioPool,
		"batch": batchPool,
	})

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerEngineServer(grpcServer, server.NewGRPCServer(r))

	log.Printf("Go Heavy Worker Engine starting...")
	log.Printf("  gRPC port: %d", port)
	log.Printf("  Metrics port: %d", metricsPort)
	log.Printf("  Pool A (CPU): %d workers", poolAWorkers)
	log.Printf("  Pool B (I/O): %d workers, rate=%d/s", poolBWorkers, poolBRateLimit)
	log.Printf("  Pool C (Batch): %d workers, rate=%d/s", poolCWorkers, poolCRateLimit)

	// Prometheus metrics HTTP server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"healthy":true}`))
		})
		log.Printf("Metrics server listening on :%d", metricsPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), mux); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// Start gRPC in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC serve error: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal %v, shutting down...", sig)

	grpcServer.GracefulStop()
	r.Stop()
	log.Println("Go Heavy Worker Engine stopped.")
}
