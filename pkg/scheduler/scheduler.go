// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/util/retry"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type (
	// Key is unique identifier of a task in scheduler.
	Key = uuid.UUID

	// Properties are externally defined task parameters.
	// They are JSON encoded.
	Properties = json.RawMessage
)

// RunContext is a bundle of Context, Key, Properties and additional runtime
// information.
type RunContext struct {
	context.Context
	Key        Key
	Properties Properties
	Retry      int8
	NoContinue bool

	err error
}

func newRunContext(key Key, properties Properties, stop time.Time) (*RunContext, context.CancelFunc) {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if stop.IsZero() {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), stop)
	}

	return &RunContext{
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
	Backoff    retry.Backoff
	Window     Window
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
	closed  bool
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

	s.details[key] = d
	// If running key will be scheduled when done in reschedule.
	if _, running := s.running[key]; running {
		s.logger.Debug(ctx, "Running scheduling deferred", "key", key)
		return
	}
	next := d.Trigger.Next(s.now())
	s.scheduleLocked(ctx, key, d, next, 0)
}

func (s *Scheduler) reschedule(ctx *RunContext) {
	key := ctx.Key

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

	var (
		now   = s.now()
		next  = d.Trigger.Next(now)
		retno int8
	)
	switch {
	case shouldContinue(ctx.err):
		next = now
		retno = ctx.Retry
	case shouldRetry(ctx.err):
		if d.Backoff != nil {
			if b := d.Backoff.NextBackOff(); b != retry.Stop {
				next = now.Add(b)
				retno = ctx.Retry + 1
				s.logger.Debug(ctx, "Retry backoff", "key", key, "backoff", b, "retry", retno)
			}
		}
	default:
		if d.Backoff != nil {
			d.Backoff.Reset()
		}
	}
	s.scheduleLocked(ctx, key, d, next, retno)
}

func shouldContinue(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func shouldRetry(err error) bool {
	return !(err == nil || errors.Is(err, context.Canceled) || retry.IsPermanent(err))
}

func (s *Scheduler) scheduleLocked(ctx context.Context, key Key, d Details, next time.Time, retno int8) {
	if next.IsZero() {
		s.logger.Info(ctx, "No triggers, removing", "key", key)
		s.unscheduleLocked(key)
		return
	}

	begin, end := d.Window.Next(next)
	if begin != next {
		s.logger.Debug(ctx, "Window aligned", "key", key, "next", begin, "end", end, "offset", begin.Sub(next))
	}

	s.logger.Info(ctx, "Schedule next", "key", key, "next", begin, "in", begin.Sub(s.now()), "retry", retno)
	a := activation{Key: key, Time: begin, Retry: retno, Stop: end}
	if s.queue.Push(a) {
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
func (s *Scheduler) Trigger(ctx context.Context, key Key, opts ...func(p Properties) Properties) bool {
	s.mu.Lock()
	s.logger.Info(ctx, "Manual trigger", "key", key)

	if _, running := s.running[key]; running {
		s.logger.Info(ctx, "Manual trigger ignored - already running", "key", key)
		s.mu.Unlock()
		return true
	}

	if s.queue.Remove(key) {
		s.wakeup()
	}
	d, ok := s.details[key]
	var runCtx *RunContext
	if ok {
		runCtx = s.newRunContextLocked(activation{Key: key})
	}
	s.mu.Unlock()
	if !ok {
		s.logger.Info(ctx, "Manual trigger ignored - unknown key", "key", key)
		return false
	}

	if len(opts) != 0 {
		p := d.Properties
		for _, o := range opts {
			p = o(p)
		}
		runCtx.Properties = p
	}

	s.asyncRun(runCtx)
	return true
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

// Close makes Start function exit, stops all runs, call Wait to wait for the
// runs to return.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.wakeup()
	for _, cancel := range s.running {
		cancel()
	}
}

// Wait waits for runs to return call after Close.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}

// Start is the scheduler main loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.logger.Info(ctx, "Scheduler started")

	for {
		var d time.Duration
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			s.logger.Debug(ctx, "Closed")
			break
		}
		top, ok := s.queue.Top()
		s.mu.Unlock()
		if !ok {
			d = -1
		} else {
			d = s.activateIn(top)
		}

		if d < 0 {
			s.logger.Debug(ctx, "Waiting for signal")
		} else {
			s.logger.Debug(ctx, "Waiting", "key", top.Key, "sleep", d)
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
		runCtx := s.newRunContextLocked(a)
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

func (s *Scheduler) newRunContextLocked(a activation) *RunContext {
	ctx, cancel := newRunContext(a.Key, s.details[a.Key].Properties, a.Stop)
	ctx.Retry = a.Retry
	s.running[a.Key] = cancel
	return ctx
}

func (s *Scheduler) asyncRun(ctx *RunContext) {
	s.logger.Info(ctx, "Run", "key", ctx.Key)

	s.wg.Add(1)
	go func(ctx *RunContext) {
		defer s.wg.Done()
		ctx.err = s.run(*ctx)
		s.onRunEnd(ctx)
		s.reschedule(ctx)
	}(ctx)
}

func (s *Scheduler) onRunEnd(ctx *RunContext) {
	err := ctx.err
	switch {
	case err == nil:
		s.logger.Info(ctx, "Success", "key", ctx.Key)
	case errors.Is(err, context.Canceled):
		s.logger.Info(ctx, "Stopped", "key", ctx.Key)
	case errors.Is(err, context.DeadlineExceeded):
		s.logger.Info(ctx, "Stopped on window end", "key", ctx.Key)
	default:
		s.logger.Info(ctx, "Failed", "key", ctx.Key, "error", err)
	}
}
