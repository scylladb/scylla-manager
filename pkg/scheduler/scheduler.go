// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// Key is unique identifier of a task in scheduler.
type Key uuid.UUID

func (k Key) String() string {
	return uuid.UUID(k).String()
}

// Properties specify task details and are passed to RunFunc.
type Properties struct {
	json.RawMessage
	NoContinue bool
}

// RunFunc specifies interface for key execution.
// When the provided context is cancelled function must return with
// context.Cancelled error, or an error caused by this error.
// Compatible functions can be passed to Scheduler constructor.
type RunFunc func(ctx context.Context, key Key, properties Properties) error

// Trigger provides the next activation date.
// Implementations must return the same values for the same now parameter.
// A zero time can be returned to indicate no more executions.
type Trigger interface {
	Next(now time.Time) time.Time
}

// Scheduler manages keys and triggers.
// A key uniquely identifies a scheduler task.
// There can be a single instance of a key scheduled or running at all times.
// Scheduler gets the next activation time for a key from a trigger.
// On key activation the RunFunc is called.
type Scheduler struct {
	now    func() time.Time
	run    RunFunc
	logger log.Logger
	timer  *time.Timer

	queue      *activationQueue
	properties map[Key]Properties
	trigger    map[Key]Trigger
	running    map[Key]context.CancelFunc
	mu         sync.Mutex

	wakeupCh chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(now func() time.Time, run RunFunc, logger log.Logger) *Scheduler {
	return &Scheduler{
		now:        now,
		run:        run,
		logger:     logger,
		timer:      time.NewTimer(0),
		queue:      newActivationQueue(),
		properties: make(map[Key]Properties),
		trigger:    make(map[Key]Trigger),
		running:    make(map[Key]context.CancelFunc),
		wakeupCh:   make(chan struct{}),
	}
}

// Schedule updates properties and trigger of an existing key or add a new key.
func (s *Scheduler) Schedule(ctx context.Context, key Key, properties Properties, trigger Trigger) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := trigger.Next(s.now())

	s.logger.Info(ctx, "Schedule", "key", key, "next", next)

	if next.IsZero() {
		s.logger.Info(ctx, "No triggers, removing", "key", key)
		s.unscheduleLocked(key)
		return
	}

	s.properties[key] = properties
	s.trigger[key] = trigger

	// If running key will be scheduled when done in reschedule.
	if _, running := s.running[key]; !running {
		if s.queue.Push(activation{Key: key, Time: next}) {
			s.wakeup()
		}
	}
}

func (s *Scheduler) reschedule(key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cancel, ok := s.running[key]
	if ok {
		cancel()
	}
	delete(s.running, key)

	t, ok := s.trigger[key]
	if !ok {
		return
	}

	next := t.Next(s.now())
	if next.IsZero() {
		s.unscheduleLocked(key)
		return
	}

	if s.queue.Push(activation{Key: key, Time: next}) {
		s.wakeup()
	}
}

// Unschedule cancels schedule of a key. It does not stop an active run.
func (s *Scheduler) Unschedule(ctx context.Context, key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info(ctx, "Unschedule", "key", key)
	s.unscheduleLocked(key)
}

func (s *Scheduler) unscheduleLocked(key Key) {
	delete(s.properties, key)
	delete(s.trigger, key)
	if s.queue.Remove(key) {
		s.wakeup()
	}
}

// Trigger immediately runs a key.
// If key is already running the call will have no effect.
// Properties can be modified for this run with options.
func (s *Scheduler) Trigger(ctx context.Context, key Key, opts ...func(p Properties) Properties) {
	s.mu.Lock()
	s.logger.Info(ctx, "Manual trigger", "key", key)

	if _, running := s.running[key]; running {
		s.logger.Info(ctx, "Manual trigger ignored - already running", "key", key)
		s.mu.Unlock()
		return
	}

	if s.queue.Remove(key) {
		s.wakeup()
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s.running[key] = cancel
	p := s.properties[key]
	s.mu.Unlock()

	for _, o := range opts {
		p = o(p)
	}

	s.asyncRun(runCtx, key, p)
}

// Stop notifies RunFunc to stop by cancelling the context.
func (s *Scheduler) Stop(ctx context.Context, key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info(ctx, "Stop", "key", key)
	if cancel, ok := s.running[key]; ok {
		cancel()
	}
}

// StopAll is equivalent to calling Stop for every running key.
func (s *Scheduler) StopAll(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info(ctx, "Stop all")
	for _, cancel := range s.running {
		cancel()
	}
}

// Start is the scheduler main loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.logger.Info(ctx, "Scheduler started")

	for {
		var d time.Duration
		s.mu.Lock()
		top, ok := s.queue.Top()
		s.mu.Unlock()
		if !ok {
			d = -1
		} else {
			d = s.activateIn(top)
		}

		if d < 0 {
			s.logger.Debug(ctx, "Waiting...")
		} else {
			s.logger.Debug(ctx, "Waiting for key", "key", top.Key, "sleep", d)
		}

		if !s.sleep(ctx, d) {
			if ctx.Err() != nil {
				s.logger.Debug(ctx, "Context canceled")
				break
			}
			continue
		}

		s.mu.Lock()
		a, ok := s.queue.Pop()
		if a != top {
			if ok {
				s.queue.Push(a)
			}
			s.mu.Unlock()
			continue
		}
		p := s.properties[a.Key]
		runCtx, cancel := context.WithCancel(context.Background())
		s.running[a.Key] = cancel
		s.mu.Unlock()

		s.asyncRun(runCtx, a.Key, p)
	}

	s.logger.Info(ctx, "Scheduler stopped")
}

func (s *Scheduler) activateIn(a activation) time.Duration {
	d := a.Sub(s.now())
	if d < 0 {
		d = 0
	}
	return d
}

// sleep waits for one of the following events: context is cancelled,
// duration expires (if d >= 0) or wakeup function is called.
// If d < 0 the timer is disabled.
// Returns true iff timer expired.
func (s *Scheduler) sleep(ctx context.Context, d time.Duration) bool {
	if !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}

	if d == 0 {
		return true
	}

	var timer <-chan time.Time
	if d > 0 {
		s.timer.Reset(d)
		timer = s.timer.C
	}

	select {
	case <-ctx.Done():
		return false
	case <-s.wakeupCh:
		return false
	case <-timer:
		return true
	}
}

func (s *Scheduler) wakeup() {
	select {
	case s.wakeupCh <- struct{}{}:
	default:
	}
}

func (s *Scheduler) asyncRun(ctx context.Context, key Key, p Properties) {
	ctx = log.WithNewTraceID(ctx)
	s.logger.Info(ctx, "Run", "key", key)

	s.wg.Add(1)
	go func(ctx context.Context, key Key, p Properties) {
		defer s.wg.Done()

		err := s.run(ctx, key, p)
		if err != nil {
			s.logger.Info(ctx, "Run failed", "key", key, "error", err)
		} else {
			s.logger.Info(ctx, "Run done", "key", key)
		}

		s.reschedule(key)
	}(ctx, key, p)
}

// Wait joins all active runs.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}
