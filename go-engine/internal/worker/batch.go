package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

// BatchHandler handles batch processing tasks.
func BatchHandler(ctx context.Context, task *pool.Task) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		payload = map[string]interface{}{}
	}

	// Simulate batch: process N items with small delay per item
	itemCount := 10
	if v, ok := payload["item_count"]; ok {
		if f, ok := v.(float64); ok {
			itemCount = int(f)
		}
	}

	processedItems := make([]string, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Small per-item work
		data := []byte(fmt.Sprintf("%s-item-%d", task.ID, i))
		hash := sha256.Sum256(data)
		processedItems = append(processedItems, fmt.Sprintf("%x", hash[:8]))

		// Small delay to simulate I/O per item
		time.Sleep(10 * time.Millisecond)
	}

	result := map[string]interface{}{
		"task_id":    task.ID,
		"item_count": itemCount,
		"processed":  len(processedItems),
		"samples":    processedItems[:min(3, len(processedItems))],
	}
	return json.Marshal(result)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
