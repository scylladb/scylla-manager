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

// Properties are externally defined task parameters.
// They are JSON encoded.
type Properties json.RawMessage

// RunContext is a bundle of Context, Key, Properties and additional runtime
// information.
type RunContext struct {
	context.Context
	Key        Key
	Properties Properties
	Retry      int8
	NoContinue bool
}

func newRunContext(key Key, properties Properties) (RunContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return RunContext{
		Context:    log.WithNewTraceID(ctx),
		Key:        key,
		Properties: properties,
	}, cancel
}

// RunFunc specifies interface for key execution.
// When the provided context is cancelled function must return with
// context.Cancelled error, or an error caused by this error.
// Compatible functions can be passed to Scheduler constructor.
type RunFunc func(ctx RunContext) error

// Trigger provides the next activation date.
// Implementations must return the same values for the same now parameter.
// A zero time can be returned to indicate no more executions.
type Trigger interface {
	Next(now time.Time) time.Time
}

// Details holds Properties, Trigger and auxiliary Key configuration.
type Details struct {
	Properties Properties
	Trigger    Trigger
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

	queue   *activationQueue
	details map[Key]Details
	running map[Key]context.CancelFunc
	mu      sync.Mutex

	wakeupCh chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(now func() time.Time, run RunFunc, logger log.Logger) *Scheduler {
	return &Scheduler{
		now:      now,
		run:      run,
		logger:   logger,
		timer:    time.NewTimer(0),
		queue:    newActivationQueue(),
		details:  make(map[Key]Details),
		running:  make(map[Key]context.CancelFunc),
		wakeupCh: make(chan struct{}),
	}
}

// Schedule updates properties and trigger of an existing key or add a new key.
func (s *Scheduler) Schedule(ctx context.Context, key Key, d Details) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := d.Trigger.Next(s.now())
	s.logger.Info(ctx, "Schedule", "key", key, "next", next)
	if next.IsZero() {
		s.logger.Info(ctx, "No triggers, removing", "key", key)
		s.unscheduleLocked(key)
		return
	}

	s.details[key] = d

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

	d, ok := s.details[key]
	if !ok {
		return
	}

	next := d.Trigger.Next(s.now())
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
	delete(s.details, key)
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
	d := s.details[key]
	runCtx, cancel := newRunContext(key, d.Properties)
	s.running[key] = cancel
	s.mu.Unlock()

	if len(opts) != 0 {
		p := d.Properties
		for _, o := range opts {
			p = o(p)
		}
		runCtx.Properties = p
	}

	s.asyncRun(runCtx)
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
		runCtx, cancel := newRunContext(a.Key, s.details[a.Key].Properties)
		s.running[a.Key] = cancel
		s.mu.Unlock()

		s.asyncRun(runCtx)
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

func (s *Scheduler) asyncRun(ctx RunContext) {
	s.logger.Info(ctx, "Run", "key", ctx.Key)

	s.wg.Add(1)
	go func(ctx RunContext) {
		defer s.wg.Done()

		if err := s.run(ctx); err != nil {
			s.logger.Info(ctx, "Run failed", "key", ctx.Key, "error", err)
		} else {
			s.logger.Info(ctx, "Run done", "key", ctx.Key)
		}

		s.reschedule(ctx.Key)
	}(ctx)
}

// Wait joins all active runs.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}
