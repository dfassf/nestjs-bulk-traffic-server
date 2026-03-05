package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dfassf/nestjs-bulk-traffic-server/go-engine/internal/pool"
)

func TestCPUHandler_Basic(t *testing.T) {
	payload, _ := json.Marshal(map[string]interface{}{"iterations": 10})
	task := &pool.Task{
		ID:      "cpu-1",
		Payload: payload,
	}

	result, err := CPUHandler(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	if parsed["task_id"] != "cpu-1" {
		t.Errorf("expected task_id cpu-1, got %v", parsed["task_id"])
	}
	if parsed["hash"] == nil || parsed["hash"] == "" {
		t.Error("expected non-empty hash")
	}
}

func TestCPUHandler_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	payload, _ := json.Marshal(map[string]interface{}{"iterations": 100000})
	task := &pool.Task{ID: "cpu-cancel", Payload: payload}

	_, err := CPUHandler(ctx, task)
	if err == nil {
		t.Error("expected cancellation error")
	}
}

func TestIOHandler_Basic(t *testing.T) {
	payload, _ := json.Marshal(map[string]interface{}{"delay_ms": 10})
	task := &pool.Task{ID: "io-1", Payload: payload}

	result, err := IOHandler(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	if parsed["status"] != "completed" {
		t.Errorf("expected completed, got %v", parsed["status"])
	}
}

func TestIOHandler_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	payload, _ := json.Marshal(map[string]interface{}{"delay_ms": 5000})
	task := &pool.Task{ID: "io-timeout", Payload: payload}

	_, err := IOHandler(ctx, task)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestBatchHandler_Basic(t *testing.T) {
	payload, _ := json.Marshal(map[string]interface{}{"item_count": 3})
	task := &pool.Task{ID: "batch-1", Payload: payload}

	result, err := BatchHandler(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	if int(parsed["processed"].(float64)) != 3 {
		t.Errorf("expected 3 processed, got %v", parsed["processed"])
	}
}

func TestBatchHandler_Cancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	payload, _ := json.Marshal(map[string]interface{}{"item_count": 1000})
	task := &pool.Task{ID: "batch-cancel", Payload: payload}

	_, err := BatchHandler(ctx, task)
	if err == nil {
		t.Error("expected cancellation error")
	}
}
