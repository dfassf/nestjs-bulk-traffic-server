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
		payload = map[string]interface{}{"data": string(task.Payload)}
	}

	// Check if findPrimes mode (max param present)
	if v, ok := payload["max"]; ok {
		if f, ok := v.(float64); ok {
			return findPrimes(ctx, int(f), task.ID)
		}
	}

	// Default: hash iterations mode
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

func findPrimes(ctx context.Context, max int, taskID string) ([]byte, error) {
	if max > 2000000 {
		max = 2000000
	}

	sieve := make([]bool, max)
	for i := range sieve {
		sieve[i] = true
	}
	if max > 0 {
		sieve[0] = false
	}
	if max > 1 {
		sieve[1] = false
	}

	for i := 2; i*i < max; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if !sieve[i] {
			continue
		}
		for j := i * i; j < max; j += i {
			sieve[j] = false
		}
	}

	count := 0
	first10 := make([]int, 0, 10)
	last10 := make([]int, 0, 10)

	for i := 2; i < max; i++ {
		if !sieve[i] {
			continue
		}
		count++
		if len(first10) < 10 {
			first10 = append(first10, i)
		}
		if len(last10) == 10 {
			last10 = last10[1:]
		}
		last10 = append(last10, i)
	}

	result := map[string]interface{}{
		"primeCount":   count,
		"first10Primes": first10,
		"last10Primes":  last10,
		"task_id":       taskID,
	}
	return json.Marshal(result)
}
