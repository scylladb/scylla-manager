// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"container/heap"
	"time"
)

type activation struct {
	time.Time
	Key        Key
	Retry      int8
	Properties Properties
	Stop       time.Time
}

// activationHeap implements heap.Interface.
// The activations are sorted by time in ascending order.
type activationHeap []activation

var _ heap.Interface = (*activationHeap)(nil)

func (h activationHeap) Len() int { return len(h) }

func (h activationHeap) Less(i, j int) bool {
	return h[i].UnixNano() < h[j].UnixNano()
}

func (h activationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *activationHeap) Push(x interface{}) {
	*h = append(*h, x.(activation))
}

func (h *activationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// activationQueue is a priority queue based on activationHeap.
// There may be only a single activation for a given activation key.
// On Push if key exists it is updated.
type activationQueue struct {
	h activationHeap
}

func newActivationQueue() *activationQueue {
	return &activationQueue{
		h: []activation{},
	}
}

// Push returns true iff head was changed.
func (q *activationQueue) Push(a activation) bool {
	if idx := q.find(a.Key); idx >= 0 {
		[]activation(q.h)[idx] = a
		heap.Fix(&q.h, idx)
	} else {
		heap.Push(&q.h, a)
	}
	return q.h[0].Key == a.Key
}

func (q *activationQueue) Pop() (activation, bool) {
	if len(q.h) == 0 {
		return activation{}, false
	}
	return heap.Pop(&q.h).(activation), true
}

func (q *activationQueue) Top() (activation, bool) {
	if len(q.h) == 0 {
		return activation{}, false
	}
	return []activation(q.h)[0], true
}

// Remove returns true iff head if head was changed.
func (q *activationQueue) Remove(key Key) bool {
	idx := q.find(key)
	if idx >= 0 {
		heap.Remove(&q.h, idx)
	}
	return idx == 0
}

func (q *activationQueue) find(key Key) int {
	for i, v := range q.h {
		if v.Key == key {
			return i
		}
	}
	return -1
}

func (q *activationQueue) Size() int {
	return len(q.h)
}
