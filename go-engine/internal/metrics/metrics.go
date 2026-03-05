package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	TasksProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_engine_tasks_processed_total",
			Help: "Total number of tasks processed",
		},
		[]string{"pool", "status"},
	)

	TaskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_engine_task_duration_ms",
			Help:    "Task processing duration in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000},
		},
		[]string{"pool"},
	)

	ActiveWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_engine_active_workers",
			Help: "Number of currently active workers",
		},
		[]string{"pool"},
	)

	QueueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_engine_queue_length",
			Help: "Current queue length",
		},
		[]string{"pool"},
	)
)

func init() {
	prometheus.MustRegister(TasksProcessed, TaskDuration, ActiveWorkers, QueueLength)
}

// Handler returns an HTTP handler for Prometheus metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
