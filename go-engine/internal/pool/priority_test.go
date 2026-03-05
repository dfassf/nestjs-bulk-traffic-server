package pool

import (
	"testing"
)

func TestPriorityQueue_EnqueueDequeue(t *testing.T) {
	pq := NewPriorityQueue()

	pq.Enqueue(&Task{ID: "low", Priority: -1})
	pq.Enqueue(&Task{ID: "high", Priority: 10})
	pq.Enqueue(&Task{ID: "normal", Priority: 1})

	// Should dequeue in priority order: high > normal > low
	task := pq.Dequeue()
	if task.ID != "high" {
		t.Errorf("expected high, got %s", task.ID)
	}

	task = pq.Dequeue()
	if task.ID != "normal" {
		t.Errorf("expected normal, got %s", task.ID)
	}

	task = pq.Dequeue()
	if task.ID != "low" {
		t.Errorf("expected low, got %s", task.ID)
	}
}

func TestPriorityQueue_DequeueEmpty(t *testing.T) {
	pq := NewPriorityQueue()
	task := pq.Dequeue()
	if task != nil {
		t.Error("expected nil from empty queue")
	}
}

func TestPriorityQueue_QueueLen(t *testing.T) {
	pq := NewPriorityQueue()
	if pq.QueueLen() != 0 {
		t.Error("expected 0 length")
	}

	pq.Enqueue(&Task{ID: "a", Priority: 1})
	pq.Enqueue(&Task{ID: "b", Priority: 2})
	if pq.QueueLen() != 2 {
		t.Errorf("expected 2, got %d", pq.QueueLen())
	}

	pq.Dequeue()
	if pq.QueueLen() != 1 {
		t.Errorf("expected 1, got %d", pq.QueueLen())
	}
}

func TestPriorityQueue_SamePriority(t *testing.T) {
	pq := NewPriorityQueue()

	pq.Enqueue(&Task{ID: "a", Priority: 5})
	pq.Enqueue(&Task{ID: "b", Priority: 5})
	pq.Enqueue(&Task{ID: "c", Priority: 5})

	// All same priority - should dequeue all 3
	count := 0
	for pq.Dequeue() != nil {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestPriorityQueue_Concurrent(t *testing.T) {
	pq := NewPriorityQueue()
	done := make(chan bool, 100)

	// Concurrent enqueues
	for i := 0; i < 100; i++ {
		go func(id int) {
			pq.Enqueue(&Task{ID: "task", Priority: int32(id % 10)})
			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	if pq.QueueLen() != 100 {
		t.Errorf("expected 100, got %d", pq.QueueLen())
	}
}
