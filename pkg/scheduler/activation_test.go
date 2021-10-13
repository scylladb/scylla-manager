// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func makeTestActivation(sec int) activation {
	return activation{
		Time: unixTime(sec),
		Key:  uuid.MustRandom(),
	}
}

func TestActivationQueue(t *testing.T) {
	t.Run("push and pop", func(t *testing.T) {
		q := newActivationQueue()
		q.Push(makeTestActivation(2))
		q.Push(makeTestActivation(1))
		a0 := makeTestActivation(0)
		q.Push(a0)
		a3 := makeTestActivation(3)
		a3.Key = a0.Key
		q.Push(a3)

		n := q.Size()
		for i := 1; i < n; i++ {
			a, ok := q.Pop()
			if !ok {
				t.Fatalf("Pop() = %v, %v, expected activation at %s", a, ok, unixTime(i))
			}
			if a.Time != unixTime(i) {
				t.Fatalf("Pop() = %v, %v, expected activation at %s", a, ok, unixTime(i))
			}
		}
	})

	t.Run("top", func(t *testing.T) {
		q := newActivationQueue()
		q.Push(makeTestActivation(2))
		q.Push(makeTestActivation(1))
		q.Push(makeTestActivation(0))

		n := q.Size()
		for i := 0; i < n; i++ {
			top, ok := q.Top()
			if !ok {
				t.Fatalf("Top() = %v, %v, expected ok to be true", top, ok)
			}
			q.Pop()
		}
	})

	t.Run("top and pop", func(t *testing.T) {
		q := newActivationQueue()

		top, ok := q.Top()
		if ok {
			t.Fatalf("top() = %v, %v, expected ok to be false", top, ok)
		}
		pop, ok := q.Pop()
		if ok {
			t.Fatalf("pop() = %v, %v, expected ok to be false", top, ok)
		}
		if pop.Key != top.Key {
			t.Fatalf("top() = %v, pop() = %v", top, pop)
		}
	})

	t.Run("find", func(t *testing.T) {
		q := newActivationQueue()
		q.Push(makeTestActivation(2))
		q.Push(makeTestActivation(1))
		q.Push(makeTestActivation(0))

		n := q.Size()
		for i := 0; i < n; i++ {
			if idx := q.find(q.h[i].Key); idx != i {
				t.Fatalf("find() = %d, expected %d", idx, i)
			}
		}
	})

	t.Run("remove", func(t *testing.T) {
		q := newActivationQueue()
		a2 := makeTestActivation(2)
		q.Push(a2)
		a1 := makeTestActivation(1)
		q.Push(a1)
		a0 := makeTestActivation(0)
		q.Push(a0)

		if ok := q.Remove(a0.Key); !ok {
			t.Fatalf("Remove()=%v, expected true", ok)
		}

		if a, _ := q.Pop(); a.Time != unixTime(1) {
			t.Fatalf("Pop() = %v, expected activation at %s", a, unixTime(1))
		}
	})
}
