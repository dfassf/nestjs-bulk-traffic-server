package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

// IOHandler handles I/O-intensive tasks.
func IOHandler(ctx context.Context, task *pool.Task) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		payload = map[string]interface{}{}
	}

	// Simulate I/O delay
	delayMs := 100
	if v, ok := payload["delay_ms"]; ok {
		if f, ok := v.(float64); ok {
			delayMs = int(f)
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(delayMs) * time.Millisecond):
	}

	result := map[string]interface{}{
		"status":   "completed",
		"delay_ms": delayMs,
		"task_id":  task.ID,
	}
	return json.Marshal(result)
}
