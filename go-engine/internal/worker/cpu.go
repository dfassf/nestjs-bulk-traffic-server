package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

// CPUHandler handles CPU-intensive tasks.
func CPUHandler(ctx context.Context, task *pool.Task) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		// If not JSON, treat as raw data for hashing
		payload = map[string]interface{}{"data": string(task.Payload)}
	}

	// Simulate CPU-intensive work: compute hash iterations
	iterations := 1000
	if v, ok := payload["iterations"]; ok {
		if f, ok := v.(float64); ok {
			iterations = int(f)
		}
	}

	data := task.Payload
	for i := 0; i < iterations; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		hash := sha256.Sum256(data)
		data = hash[:]
	}

	result := map[string]interface{}{
		"hash":       fmt.Sprintf("%x", data),
		"iterations": iterations,
		"task_id":    task.ID,
	}
	return json.Marshal(result)
}
