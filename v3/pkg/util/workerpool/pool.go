// Copyright (C) 2023 ScyllaDB

package workerpool

import (
	"context"
	"sync"
)

// JobHandler describes handler for tasks of type T which returns results of type R.
type JobHandler[T, R any] interface {
	// HandleJob is called when worker receives task from the pool.
	HandleJob(ctx context.Context, task T) R
	// Done is called when worker exits.
	Done(ctx context.Context)
}

// Pool allows for executing homogenous JobHandlers with ongoing control over parallelism.
// Use Spawn, Kill and SetSize methods to change the current amount of workers.
// Use Submit to submit new task and Results to wait for the results.
// Pool can be closed with Close or drained with cancelling workers context.
type Pool[W JobHandler[T, R], T, R any] struct {
	workerCtx   context.Context // nolint: containedctx
	spawnWorker func(ctx context.Context, id int) W

	tasks      chan T
	results    chan R
	killWorker chan struct{}
	size       int
	totalSize  int
	wait       sync.WaitGroup
	closed     bool
	mu         sync.Mutex
}

// New returns newly created Pool.
// WorkerCtx is passed to ever HandleJob execution and can be cancelled
// to shut down the pool.
// Spawn function is used for creating new JobHandlers with unique ID.
// ChanSize describes the max number of unprocessed tasks and results
// before operations start blocking.
func New[W JobHandler[T, R], T, R any](workerCtx context.Context, spawn func(context.Context, int) W, chanSize int) *Pool[W, T, R] {
	return &Pool[W, T, R]{
		workerCtx:   workerCtx,
		spawnWorker: spawn,
		tasks:       make(chan T, chanSize),
		results:     make(chan R, chanSize),
		killWorker:  make(chan struct{}, chanSize),
		size:        0,
		totalSize:   0,
		wait:        sync.WaitGroup{},
		closed:      false,
		mu:          sync.Mutex{},
	}
}

// Spawn adds a single worker to the pool.
func (p *Pool[_, _, _]) Spawn() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.spawn()
}

func (p *Pool[_, _, _]) spawn() {
	p.wait.Add(1)
	p.size++
	p.totalSize++
	id := p.totalSize
	w := p.spawnWorker(p.workerCtx, id)

	go func() {
		defer func() {
			p.wait.Done()
			w.Done(p.workerCtx)
		}()

		for {
			select {
			case <-p.killWorker:
				return
			default:
			}
			if p.workerCtx.Err() != nil {
				return
			}

			select {
			case <-p.workerCtx.Done():
				return
			case <-p.killWorker:
				return
			case t, ok := <-p.tasks:
				if !ok {
					return
				}
				p.results <- w.HandleJob(p.workerCtx, t)
			}
		}
	}()
}

func (p *Pool[_, _, _]) kill() {
	p.killWorker <- struct{}{}
	p.size--
}

// Kill removes single worker from the pool.
func (p *Pool[_, _, _]) Kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.kill()
}

// SetSize spawns or kills worker to achieve desired pool size.
func (p *Pool[_, _, _]) SetSize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}

	for p.size > size {
		p.kill()
	}
	for p.size < size {
		p.spawn()
	}
}

// Submit queues task to be picked up by a free worker.
// Might block when chanSize is too little.
func (p *Pool[_, T, _]) Submit(task T) {
	p.tasks <- task
}

// Results returns channel that can be used to wait for
// the results of submitted tasks.
func (p *Pool[_, _, R]) Results() chan R {
	return p.results
}

// Size returns the current number of workers.
func (p *Pool[_, _, _]) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.size
}

// Close kills all workers and makes submitting new tasks panic.
func (p *Pool[_, _, _]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	close(p.killWorker)
	close(p.tasks)
	p.size = 0
	p.closed = true
}

// Wait returns when all workers have exited.
func (p *Pool[_, _, _]) Wait() {
	p.wait.Wait()
}
