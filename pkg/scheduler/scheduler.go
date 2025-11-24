// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
)

// Properties are externally defined task parameters.
type Properties = any

// RunContext is a bundle of Context, Key, Properties and additional runtime
// information.
type RunContext[K comparable] struct {
	context.Context //nolint:containedctx

	Key        K
	Properties Properties
	Retry      int8

	err error
}

// Errors describing the cause of task interruption.
var (
	ErrStoppedTask      = errors.New("task was stopped")
	ErrOutOfWindowTask  = errors.New("task got out of --window")
	ErrRescheduledTask  = errors.New("task is being rescheduled")
	ErrStoppedScheduler = errors.New("scheduler and all tasks were stopped")
)

func newRunContext[K comparable](key K, properties Properties, stop time.Time) (*RunContext[K], context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(context.Background())
	if !stop.IsZero() {
		ctx, _ = context.WithDeadlineCause(ctx, stop, ErrOutOfWindowTask) // nolint:govet
	}

	return &RunContext[K]{
		Context:    ctx,
		Key:        key,
		Properties: properties,
	}, cancel
}

// RunFunc specifies interface for key execution.
// When the provided context is cancelled function must return with
// context.Cancelled error, or an error caused by this error.
// Compatible functions can be passed to Scheduler constructor.
type RunFunc[K comparable] func(ctx RunContext[K]) error

// Details holds Properties, Trigger and auxiliary Key configuration.
type Details struct {
	Properties Properties
	Trigger    schedules.Trigger
	Backoff    retry.Backoff
	Window     Window
	Location   *time.Location
}

// Scheduler manages keys and triggers.
// A key uniquely identifies a scheduler task.
// There can be a single instance of a key scheduled or running at all times.
// Scheduler gets the next activation time for a key from a trigger.
// On key activation the RunFunc is called.
type Scheduler[K comparable] struct {
	now      func() time.Time
	run      RunFunc[K]
	listener Listener[K]
	timer    *time.Timer

	queue   *activationQueue[K]
	details map[K]Details
	running map[K]context.CancelCauseFunc
	closed  bool
	mu      sync.Mutex

	wakeupCh chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler[K comparable](now func() time.Time, run RunFunc[K], listener Listener[K]) *Scheduler[K] {
	return &Scheduler[K]{
		now:      now,
		run:      run,
		listener: listener,
		timer:    time.NewTimer(0),
		queue:    newActivationQueue[K](),
		details:  make(map[K]Details),
		running:  make(map[K]context.CancelCauseFunc),
		wakeupCh: make(chan struct{}, 1),
	}
}

// Schedule updates properties and trigger of an existing key or adds a new key.
func (s *Scheduler[K]) Schedule(ctx context.Context, key K, d Details) {
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

	s.scheduleLocked(ctx, key, next, time.Time{}, 0, nil, d.Window)
}

func (s *Scheduler[K]) reschedule(ctx *RunContext[K], initialActivation time.Time) {
	key := ctx.Key

	s.mu.Lock()
	defer s.mu.Unlock()

	cancel, ok := s.running[key]
	if ok {
		cancel(ErrRescheduledTask)
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
		retno                   int8
		p                       Properties
		preRescheduleActivation time.Time
	)
	switch {
	case shouldContinue(ctx):
		next = now
		retno = ctx.Retry
		p = ctx.Properties
		preRescheduleActivation = initialActivation
	case shouldRetry(ctx, ctx.err):
		if d.Backoff != nil {
			if b := d.Backoff.NextBackOff(); b != retry.Stop {
				next = now.Add(b)
				retno = ctx.Retry + 1
				p = ctx.Properties
				s.listener.OnRetryBackoff(ctx, key, b, retno)
				preRescheduleActivation = initialActivation
			}
		}
	default:
		// Use initial activation time instead of now so that we don't skip
		// activations of runs which ran longer than cron interval (#4309).
		//
		// Reschedule after long run:
		// Cron activation: |-A-------A-------A------ ...
		// Task execution:  |-[EEEEEEEE][EEE]-[EEE]-- ...
		//
		// Reschedule after retries:
		// Cron activation:      |-A-------A-------A----- ...
		// Task execution/retry: |-[E]-[R]-[R][EE]-[EE]-- ...
		//
		// Reschedule after maintenance window:
		// Cron activation:         |-A-------A-------A----- ...
		// Maintenance window:      |[WW]-[W]---[WWWWWWWWWWW ...
		// Task execution/continue: |-[E]-[C]---[C][E][E]--- ...
		//
		// Note that initial activation run is not calculated for runs interrupted
		// by pause/start and suspend/resume, as they are treated as fresh runs by
		// the scheduler.
		//
		// In general, if task execution takes more time than cron interval,
		// then the problem is on the cron definition side, but we should still
		// try to alleviate this issue for "spontaneous" long task executions.
		//
		// The +1 should ensure that next is strictly after activation time.
		// In case this assertion fails, fallback to the previous scheduling
		// mechanism which uses now for calculating next activation time.
		if a := d.Trigger.Next(initialActivation.Add(1)); a.After(initialActivation) {
			next = a
		}
		if d.Backoff != nil {
			d.Backoff.Reset()
		}
	}
	s.scheduleLocked(ctx, key, next, preRescheduleActivation, retno, p, d.Window)
}

func shouldContinue(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrOutOfWindowTask)
}

func shouldRetry(ctx context.Context, err error) bool {
	return !(err == nil || errors.Is(context.Cause(ctx), ErrStoppedTask) || retry.IsPermanent(err))
}

func (s *Scheduler[K]) scheduleLocked(ctx context.Context, key K, next, preRescheduleActivation time.Time, retno int8, p Properties, w Window) {
	if next.IsZero() {
		s.listener.OnNoTrigger(ctx, key)
		s.unscheduleLocked(key)
		return
	}

	begin, end := w.Next(next)

	s.listener.OnSchedule(ctx, key, begin, end, retno)
	a := Activation[K]{
		Time:                    begin,
		Key:                     key,
		Retry:                   retno,
		Properties:              p,
		Stop:                    end,
		PreRescheduleActivation: preRescheduleActivation,
	}
	if s.queue.Push(a) {
		s.wakeup()
	}
}

// Unschedule cancels schedule of a key. It does not stop an active run.
func (s *Scheduler[K]) Unschedule(ctx context.Context, key K) {
	s.listener.OnUnschedule(ctx, key)
	s.mu.Lock()
	s.unscheduleLocked(key)
	s.mu.Unlock()
}

func (s *Scheduler[K]) unscheduleLocked(key K) {
	delete(s.details, key)
	if s.queue.Remove(key) {
		s.wakeup()
	}
}

// Trigger immediately runs a scheduled key.
// If key is already running the call will have no effect and true is returned.
// If key is not scheduled the call will have no effect and false is returned.
func (s *Scheduler[K]) Trigger(ctx context.Context, key K) bool {
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
	var runCtx *RunContext[K]
	if ok {
		runCtx = s.newRunContextLocked(Activation[K]{Key: key})
	}
	s.mu.Unlock()

	s.listener.OnTrigger(ctx, key, ok)
	if ok {
		s.asyncRun(runCtx, s.now())
	}
	return ok
}

// Stop notifies RunFunc to stop by cancelling the context.
func (s *Scheduler[K]) Stop(ctx context.Context, key K) {
	s.listener.OnStop(ctx, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.running[key]; ok {
		cancel(ErrStoppedTask)
	}
}

// Close makes Start function exit, stops all runs, call Wait to wait for the
// runs to return.
// It returns two sets of keys the running that were canceled and pending that
// were scheduled to run.
func (s *Scheduler[K]) Close() (running, pending []K) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.wakeup()
	for k, cancel := range s.running {
		running = append(running, k)
		cancel(ErrStoppedScheduler)
	}
	for _, a := range s.queue.h {
		pending = append(pending, a.Key)
	}
	return
}

// Wait waits for runs to return call after Close.
func (s *Scheduler[_]) Wait() {
	s.wg.Wait()
}

// Activations returns activation information for given keys.
func (s *Scheduler[K]) Activations(keys ...K) []Activation[K] {
	pos := make(map[K]int, len(keys))
	for i, k := range keys {
		pos[k] = i
	}
	r := make([]Activation[K], len(keys))
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
func (s *Scheduler[_]) Start(ctx context.Context) {
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

		s.asyncRun(runCtx, InitialActivation(a))
	}

	s.listener.OnSchedulerStop(ctx)
}

func (s *Scheduler[K]) activateIn(a Activation[K]) time.Duration {
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
func (s *Scheduler[_]) sleep(ctx context.Context, d time.Duration) bool {
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

func (s *Scheduler[_]) wakeup() {
	select {
	case s.wakeupCh <- struct{}{}:
	default:
	}
}

func (s *Scheduler[K]) newRunContextLocked(a Activation[K]) *RunContext[K] {
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

func (s *Scheduler[K]) asyncRun(ctx *RunContext[K], initialActivation time.Time) {
	s.listener.OnRunStart(ctx)
	s.wg.Add(1)
	go func(ctx *RunContext[K]) {
		defer s.wg.Done()
		ctx.err = s.run(*ctx)
		s.onRunEnd(ctx)
		s.reschedule(ctx, initialActivation)
	}(ctx)
}

func (s *Scheduler[K]) onRunEnd(ctx *RunContext[K]) {
	err := ctx.err
	switch {
	case err == nil:
		s.listener.OnRunSuccess(ctx)
	case errors.Is(context.Cause(ctx), ErrStoppedTask):
		s.listener.OnRunStop(ctx, err)
	case errors.Is(context.Cause(ctx), ErrOutOfWindowTask):
		s.listener.OnRunWindowEnd(ctx, err)
	default:
		s.listener.OnRunError(ctx, err)
	}
}

// IsTaskInterrupted returns true if task execution was interrupted by scheduler.
func IsTaskInterrupted(ctx context.Context) bool {
	schedulerErrs := []error{ErrStoppedTask, ErrOutOfWindowTask, ErrRescheduledTask, ErrStoppedScheduler}
	err := context.Cause(ctx)
	for _, schedulerErr := range schedulerErrs {
		if errors.Is(err, schedulerErr) {
			return true
		}
	}
	return false
}
