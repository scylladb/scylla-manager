// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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
		Context:    ctx,
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
	Location   *time.Location
}

// Scheduler manages keys and triggers.
// A key uniquely identifies a scheduler task.
// There can be a single instance of a key scheduled or running at all times.
// Scheduler gets the next activation time for a key from a trigger.
// On key activation the RunFunc is called.
type Scheduler struct {
	now      func() time.Time
	run      RunFunc
	listener Listener
	timer    *time.Timer

	queue   *activationQueue
	details map[Key]Details
	running map[Key]context.CancelFunc
	closed  bool
	mu      sync.Mutex

	wakeupCh chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(now func() time.Time, run RunFunc, listener Listener) *Scheduler {
	return &Scheduler{
		now:      now,
		run:      run,
		listener: listener,
		timer:    time.NewTimer(0),
		queue:    newActivationQueue(),
		details:  make(map[Key]Details),
		running:  make(map[Key]context.CancelFunc),
		wakeupCh: make(chan struct{}, 1),
	}
}

// Schedule updates properties and trigger of an existing key or adds a new key.
func (s *Scheduler) Schedule(ctx context.Context, key Key, d Details) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.details[key] = d
	// If running key will be scheduled when done in reschedule.
	if _, running := s.running[key]; running {
		return
	}

	now := s.now()
	if d.Location != nil {
		now = now.In(d.Location)
	}
	next := d.Trigger.Next(now)

	s.scheduleLocked(ctx, key, next, 0, nil, d.Window)
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

	if s.closed {
		return
	}
	d, ok := s.details[key]
	if !ok {
		return
	}

	now := s.now()
	if d.Location != nil {
		now = now.In(d.Location)
	}
	next := d.Trigger.Next(now)

	var (
		retno int8
		p     Properties
	)
	switch {
	case shouldContinue(ctx.err):
		next = now
		retno = ctx.Retry
		p = ctx.Properties
	case shouldRetry(ctx.err):
		if d.Backoff != nil {
			if b := d.Backoff.NextBackOff(); b != retry.Stop {
				next = now.Add(b)
				retno = ctx.Retry + 1
				p = ctx.Properties
				s.listener.OnRetryBackoff(ctx, key, b, retno)
			}
		}
	default:
		if d.Backoff != nil {
			d.Backoff.Reset()
		}
	}
	s.scheduleLocked(ctx, key, next, retno, p, d.Window)
}

func shouldContinue(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func shouldRetry(err error) bool {
	return !(err == nil || errors.Is(err, context.Canceled) || retry.IsPermanent(err))
}

func (s *Scheduler) scheduleLocked(ctx context.Context, key Key, next time.Time, retno int8, p Properties, w Window) {
	if next.IsZero() {
		s.listener.OnNoTrigger(ctx, key)
		s.unscheduleLocked(key)
		return
	}

	begin, end := w.Next(next)

	s.listener.OnSchedule(ctx, key, begin, end, retno)
	a := Activation{Key: key, Time: begin, Retry: retno, Properties: p, Stop: end}
	if s.queue.Push(a) {
		s.wakeup()
	}
}

// Unschedule cancels schedule of a key. It does not stop an active run.
func (s *Scheduler) Unschedule(ctx context.Context, key Key) {
	s.listener.OnUnschedule(ctx, key)
	s.mu.Lock()
	s.unscheduleLocked(key)
	s.mu.Unlock()
}

func (s *Scheduler) unscheduleLocked(key Key) {
	delete(s.details, key)
	if s.queue.Remove(key) {
		s.wakeup()
	}
}

// Trigger immediately runs a scheduled key.
// If key is already running the call will have no effect and true is returned.
// If key is not scheduled the call will have no effect and false is returned.
func (s *Scheduler) Trigger(ctx context.Context, key Key) bool {
	s.mu.Lock()
	if _, running := s.running[key]; running {
		s.mu.Unlock()
		s.listener.OnTrigger(ctx, key, true)
		return true
	}

	if s.queue.Remove(key) {
		s.wakeup()
	}
	_, ok := s.details[key]
	var runCtx *RunContext
	if ok {
		runCtx = s.newRunContextLocked(Activation{Key: key})
	}
	s.mu.Unlock()

	s.listener.OnTrigger(ctx, key, ok)
	if ok {
		s.asyncRun(runCtx)
	}
	return ok
}

// Stop notifies RunFunc to stop by cancelling the context.
func (s *Scheduler) Stop(ctx context.Context, key Key) {
	s.listener.OnStop(ctx, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.running[key]; ok {
		cancel()
	}
}

// Close makes Start function exit, stops all runs, call Wait to wait for the
// runs to return.
// It returns two sets of keys the running that were canceled and pending that
// were scheduled to run.
func (s *Scheduler) Close() (running, pending []Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.wakeup()
	for k, cancel := range s.running {
		running = append(running, k)
		cancel()
	}
	for _, a := range s.queue.h {
		pending = append(pending, a.Key)
	}
	return
}

// Wait waits for runs to return call after Close.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}

// Activations returns activation information for given keys.
func (s *Scheduler) Activations(keys ...Key) []Activation {
	pos := make(map[Key]int, len(keys))
	for i, k := range keys {
		pos[k] = i
	}
	r := make([]Activation, len(keys))
	s.mu.Lock()
	for _, a := range s.queue.h {
		if i, ok := pos[a.Key]; ok {
			r[i] = a
		}
	}
	s.mu.Unlock()
	return r
}

// Start is the scheduler main loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.listener.OnSchedulerStart(ctx)

	for {
		var d time.Duration
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			break
		}
		top, ok := s.queue.Top()
		s.mu.Unlock()

		if !ok {
			d = -1
		} else {
			d = s.activateIn(top)
		}
		s.listener.OnSleep(ctx, top.Key, d)
		if !s.sleep(ctx, d) {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		s.mu.Lock()
		a, ok := s.queue.Pop()
		if a.Key != top.Key {
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

	s.listener.OnSchedulerStop(ctx)
}

func (s *Scheduler) activateIn(a Activation) time.Duration {
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

func (s *Scheduler) newRunContextLocked(a Activation) *RunContext {
	var p Properties
	if a.Properties != nil {
		p = a.Properties
	} else {
		p = s.details[a.Key].Properties
	}

	ctx, cancel := newRunContext(a.Key, p, a.Stop)
	ctx.Retry = a.Retry
	s.running[a.Key] = cancel
	return ctx
}

func (s *Scheduler) asyncRun(ctx *RunContext) {
	s.listener.OnRunStart(ctx)
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
		s.listener.OnRunSuccess(ctx)
	case errors.Is(err, context.Canceled):
		s.listener.OnRunStop(ctx)
	case errors.Is(err, context.DeadlineExceeded):
		s.listener.OnRunWindowEnd(ctx)
	default:
		s.listener.OnRunError(ctx, err)
	}
}
