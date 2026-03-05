package pool

import (
	"container/heap"
	"sync"
)

// Task represents a unit of work with priority.
type Task struct {
	ID          string
	Priority    int32 // >= 5: high, >= 0: normal, < 0: low
	Payload     []byte
	TimeoutMs   int64
	Metadata    map[string]string
	WorkloadType string

	index int // heap internal
}

// PriorityQueue implements heap.Interface for Task items.
type PriorityQueue struct {
	mu    sync.Mutex
	items []*Task
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(pq)
	return pq
}

func (pq *PriorityQueue) Len() int { return len(pq.items) }

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.items[i].Priority > pq.items[j].Priority // higher priority first
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*Task)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	pq.items = old[:n-1]
	return item
}

// Enqueue adds a task thread-safely.
func (pq *PriorityQueue) Enqueue(task *Task) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq, task)
}

// Dequeue removes and returns the highest-priority task, or nil.
func (pq *PriorityQueue) Dequeue() *Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.items) == 0 {
		return nil
	}
	return heap.Pop(pq).(*Task)
}

// QueueLen returns the current queue length.
func (pq *PriorityQueue) QueueLen() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}
