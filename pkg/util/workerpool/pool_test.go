// Copyright (C) 2023 ScyllaDB

package workerpool

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/atomic"
)

var (
	totalCtr = atomic.NewInt64(0)
	currCtr  = atomic.NewInt64(0)
	chanSize = 1024
)

type task time.Duration
type result int

type worker struct {
	id int
}

func spawn(ctx context.Context, i int) *worker {
	return &worker{i}
}
func (w *worker) HandleJob(_ context.Context, t task) result {
	totalCtr.Inc()
	currCtr.Inc()
	time.Sleep(time.Duration(t))
	currCtr.Dec()
	return result(w.id)
}

func (w *worker) Done(_ context.Context) {}

func TestPoolSmoke(t *testing.T) {
	var (
		ctx       = context.Background()
		workerCnt = 200
		p         = New[*worker, task, result](ctx, spawn, chanSize)
	)
	p.SetSize(workerCnt)

	for i := 0; i < workerCnt; i++ {
		p.Submit(task(time.Second))
	}

	time.Sleep(50 * time.Millisecond)
	// Assert that all workers are processing jobs in parallel
	if v := currCtr.Load(); v != int64(workerCnt) {
		t.Fatalf("All workers should be working, expected: %d, got: %d", workerCnt, v)
	}

	time.Sleep(time.Second)
	m := make(map[result]struct{})
	for len(p.Results()) > 0 {
		m[<-p.Results()] = struct{}{}
	}
	// Assert that all workers produced results
	for i := 1; i <= workerCnt; i++ {
		if _, ok := m[result(i)]; !ok {
			t.Fatalf("Missing finished worker: %d", i)
		}
	}

	p.SetSize(0)

	time.Sleep(time.Second)
	// Assert that all workers are killed
	if v := p.Size(); v != 0 {
		t.Fatalf("No worker should be working, expected: %d, got: %d", workerCnt, v)
	}
}

func TestPoolSpawnKill(t *testing.T) {
	var (
		ctx              = context.Background()
		remainingWorkers = 100
		killCnt          = remainingWorkers
		workerCnt        = 2 * killCnt
		p                = New[*worker, task, result](ctx, spawn, chanSize)
	)

	// Spawn and kill workers in random order
	for 0 < workerCnt || 0 < killCnt {
		if killCnt == 0 || (workerCnt > 0 && rand.Int()%2 == 0) {
			workerCnt--
			p.spawn()
		} else {
			killCnt--
			p.kill()
		}
	}

	time.Sleep(10 * time.Millisecond)
	// Assert that the correct number of workers is still alive
	if v := p.Size(); v != remainingWorkers {
		t.Fatalf("Half of the workers should be alive, expected: %d, got: %d", remainingWorkers, v)
	}
}

func TestPoolCtxCancel(t *testing.T) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		workerCnt   = 50
		taskCnt     = chanSize
		p           = New[*worker, task, result](ctx, spawn, chanSize)
	)

	total := totalCtr.Load()
	p.SetSize(workerCnt)
	for i := 0; i < taskCnt; i++ {
		if i == workerCnt {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}
		p.Submit(task(100 * time.Millisecond))
	}

	p.Wait()
	// Assert that tasks submitted after context cancel weren't executed
	if v := totalCtr.Load() - total; v != int64(workerCnt) {
		t.Fatalf("Tasks executed after cotext cancel, expected: %d, got: %d", workerCnt, v)
	}
}

func TestPoolClose(t *testing.T) {
	var (
		ctx       = context.Background()
		workerCnt = 50
		p         = New[*worker, task, result](ctx, spawn, chanSize)
	)

	p.SetSize(workerCnt)
	for i := 0; i < 2*workerCnt; i++ {
		p.Submit(task(100 * time.Millisecond))
	}
	p.Close()
	p.Wait()
}
