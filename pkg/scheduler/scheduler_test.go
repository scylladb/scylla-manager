// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

const (
	Timeout     = 1 * time.Second
	StartOffset = 10 * time.Millisecond
)

type (
	testKey        = uuid.UUID
	testScheduler  = Scheduler[testKey]
	testRunContext = RunContext[testKey]
	testRunFunc    = RunFunc[testKey]
)

func unixTime(sec int) time.Time {
	return time.Unix(int64(sec), 0)
}

func relativeTime() func() time.Time {
	start := timeutc.Now()
	return func() time.Time {
		return unixTime(0).Add(timeutc.Since(start))
	}
}

func startAndWait(ctx context.Context, s *testScheduler) chan struct{} {
	ch := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(ch)
	}()
	return ch
}

type fakeRunner struct {
	c *atomic.Int64
	C chan testRunContext
	F testRunFunc
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		c: atomic.NewInt64(0),
		C: make(chan testRunContext),
	}
}

func (r *fakeRunner) Run(ctx testRunContext) error {
	r.c.Inc()
	defer func() {
		r.C <- ctx
	}()
	if r.F != nil {
		return r.F(ctx)
	}
	return nil
}

func (r *fakeRunner) WaitNKeys(n int) chan struct{} {
	ch := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			<-r.C
		}
		close(ch)
	}()
	return ch
}

func (r *fakeRunner) WaitKeys(keys ...testKey) chan struct{} {
	return r.WaitKeysCheckContext(nil, keys...)
}

func (r *fakeRunner) WaitKeysCheckContext(check func(runCtx testRunContext) error, keys ...testKey) chan struct{} {
	ch := make(chan struct{})
	go func() {
		for i := 0; i < len(keys); i++ {
			runCtx := <-r.C
			if runCtx.Key != keys[i] {
				msg := fmt.Sprintf("Run key=%s, expected %s", runCtx.Key, keys[i])
				panic(msg)
			}
			if check != nil {
				if err := check(runCtx); err != nil {
					msg := fmt.Sprintf("Run key=%s check error %s", runCtx.Key, err)
					panic(msg)
				}
			}
		}
		close(ch)
	}()
	return ch
}

func (r *fakeRunner) Wait(d time.Duration) chan struct{} {
	ch := make(chan struct{})
	go func() {
		t := time.NewTimer(d)
		defer t.Stop()
	loop:
		for {
			select {
			case <-r.C:
			case <-t.C:
				break loop
			}
		}
		close(ch)
	}()
	return ch
}

func (r *fakeRunner) Count() int {
	return int(r.c.Load())
}

func randomKey() testKey {
	return testKey(uuid.MustRandom())
}

func randomKeys(n int) []testKey {
	keys := make([]testKey, n)
	for i := range keys {
		keys[i] = randomKey()
	}
	return keys
}

type fakeTrigger struct {
	a []time.Time
}

func newFakeTrigger(d ...time.Duration) fakeTrigger {
	z := unixTime(0)
	a := make([]time.Time, len(d))
	for i := range d {
		a[i] = z.Add(d[i])
	}
	return fakeTrigger{
		a: a,
	}
}

func newFakeTriggerWithTime(t ...time.Time) fakeTrigger {
	return fakeTrigger{a: t}
}

func (f fakeTrigger) Next(now time.Time) time.Time {
	for _, t := range f.a {
		if now.Before(t) {
			return t
		}
	}
	return time.Time{}
}

func details(t Trigger) Details {
	return Details{
		Trigger: t,
	}
}

type logListener struct {
	logger log.Logger
}

func (l logListener) OnSchedulerStart(ctx context.Context) {
	l.logger.Info(ctx, "OnSchedulerStart")
}

func (l logListener) OnSchedulerStop(ctx context.Context) {
	l.logger.Info(ctx, "OnSchedulerStop")
}

func (l logListener) OnRunStart(ctx *testRunContext) {
	l.logger.Info(ctx, "OnRunStart", "key", ctx.Key, "retry", ctx.Retry)
}

func (l logListener) OnRunSuccess(ctx *testRunContext) {
	l.logger.Info(ctx, "OnRunSuccess", "key", ctx.Key, "retry", ctx.Retry)
}

func (l logListener) OnRunStop(ctx *testRunContext) {
	l.logger.Info(ctx, "OnRunStop", "key", ctx.Key, "retry", ctx.Retry)
}

func (l logListener) OnRunWindowEnd(ctx *testRunContext) {
	l.logger.Info(ctx, "OnRunWindowEnd", "key", ctx.Key, "retry", ctx.Retry)
}

func (l logListener) OnRunError(ctx *testRunContext, err error) {
	l.logger.Info(ctx, "OnRunError", "key", ctx.Key, "retry", ctx.Retry, "error", err)
}

func (l logListener) OnSchedule(ctx context.Context, key testKey, begin, end time.Time, retno int8) {
	l.logger.Info(ctx, "OnSchedule", "key", key, "begin", begin, "end", end, "retry", retno)
}

func (l logListener) OnUnschedule(ctx context.Context, key testKey) {
	l.logger.Info(ctx, "OnUnschedule", "key", key)
}

func (l logListener) OnTrigger(ctx context.Context, key testKey, success bool) {
	l.logger.Info(ctx, "OnTrigger", "key", key, "success", success)
}

func (l logListener) OnStop(ctx context.Context, key testKey) {
	l.logger.Info(ctx, "OnStop", "key", key)
}

func (l logListener) OnRetryBackoff(ctx context.Context, key testKey, backoff time.Duration, retno int8) {
	l.logger.Info(ctx, "OnRetryBackoff", "key", key, "backoff", retno)
}

func (l logListener) OnNoTrigger(ctx context.Context, key testKey) {
	l.logger.Info(ctx, "OnNoTrigger", "key", key)
}

func (l logListener) OnSleep(ctx context.Context, key testKey, d time.Duration) {
	l.logger.Info(ctx, "OnSleep", "key", key, "duration", d)
}

var ll = logListener{
	logger: log.NewDevelopment(),
}

func TestStopEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)

	time.AfterFunc(StartOffset, cancel)
	select {
	case <-startAndWait(ctx, s):
	case <-time.After(Timeout):
		t.Fatal("expected scheduler to be stopped")
	}
}

func TestScheduleAfterStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)

	time.AfterFunc(StartOffset, func() {
		cancel()
		s.Schedule(ctx, randomKey(), details(newFakeTrigger(0)))
	})

	select {
	case <-startAndWait(ctx, s):
	case <-time.After(Timeout):
		t.Fatal("expected scheduler to be stopped")
	}
	if f.Count() != 0 {
		t.Fatal("unexpected run")
	}
}

func TestScheduleBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	s.Schedule(ctx, randomKey(), details(newFakeTrigger(100*time.Millisecond)))

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.C:
	}

	if f.Count() != 1 {
		t.Fatalf("Count()=%d, expected %d", f.Count(), 1)
	}
}

func TestScheduleWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKey()
	s.Schedule(ctx, k, details(newFakeTrigger(50*time.Millisecond)))

	time.AfterFunc(100*time.Millisecond, func() {
		s.Schedule(ctx, k, Details{
			Trigger: newFakeTrigger(300 * time.Millisecond),
		})
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k, k):
	}
}

func TestStartSchedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)

	time.AfterFunc(StartOffset, func() {
		s.Schedule(ctx, randomKey(), details(newFakeTrigger(100*time.Millisecond)))
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.C:
	}

	if f.Count() != 1 {
		t.Fatalf("Count()=%d, expected %d", f.Count(), 1)
	}
}

func TestUnscheduleBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKey()
	s.Schedule(ctx, k, details(newFakeTrigger(500*time.Millisecond)))

	time.AfterFunc(StartOffset, func() {
		s.Unschedule(ctx, k)
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-f.C:
		t.Fatal("unexpected run")
	case <-time.After(Timeout):
	}
}

func TestUnschduleHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(3)
	s.Schedule(ctx, k[0], details(newFakeTrigger(500*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(600*time.Millisecond)))
	s.Schedule(ctx, k[2], details(newFakeTrigger(700*time.Millisecond)))

	time.AfterFunc(StartOffset, func() {
		s.Unschedule(ctx, k[0])
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[1], k[2]):
	}
}

func TestUnschduleTail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(3)
	s.Schedule(ctx, k[0], details(newFakeTrigger(500*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(600*time.Millisecond)))
	s.Schedule(ctx, k[2], details(newFakeTrigger(700*time.Millisecond)))

	time.AfterFunc(StartOffset, func() {
		s.Unschedule(ctx, k[1])
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[0], k[2]):
	}
}

func TestRescheduleHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(3)
	s.Schedule(ctx, k[0], details(newFakeTrigger(500*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(600*time.Millisecond)))
	s.Schedule(ctx, k[2], details(newFakeTrigger(700*time.Millisecond)))

	time.AfterFunc(StartOffset, func() {
		s.Schedule(ctx, k[0], details(newFakeTrigger(800*time.Millisecond)))
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[1], k[2], k[0]):
	}
}

func TestRescheduleInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(2)
	s.Schedule(ctx, k[0], details(newFakeTrigger(100*time.Millisecond, 300*time.Millisecond, 400*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(200*time.Millisecond)))

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[0], k[1], k[0], k[0]):
	}
}

func TestTriggerHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(3)
	s.Schedule(ctx, k[0], details(newFakeTrigger(500*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(600*time.Millisecond)))
	s.Schedule(ctx, k[2], details(newFakeTrigger(700*time.Millisecond)))

	time.AfterFunc(StartOffset, func() {
		s.Trigger(ctx, k[0])
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[0], k[0], k[1], k[2]):
	}
}

func TestTriggerUnknownKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKey()

	time.AfterFunc(StartOffset, func() {
		s.Trigger(ctx, k)
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(100 * time.Millisecond):
		if f.Count() != 0 {
			t.Fatal("Unexpected run")
		}
	}
}

func TestStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(2)
	s.Schedule(ctx, k[0], details(newFakeTrigger(100*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(200*time.Millisecond)))

	time.AfterFunc(150*time.Millisecond, func() {
		s.Stop(ctx, k[0])
	})
	time.AfterFunc(300*time.Millisecond, func() {
		s.Stop(ctx, k[1])
	})

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k[0], k[1]):
	}
}

func TestCloseAndWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKeys(2)
	s.Schedule(ctx, k[0], details(newFakeTrigger(100*time.Millisecond)))
	s.Schedule(ctx, k[1], details(newFakeTrigger(200*time.Millisecond)))

	time.AfterFunc(300*time.Millisecond, func() {
		s.Close()
		s.Wait()
	})

	select {
	case <-startAndWait(ctx, s):
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	}

	select {
	case <-f.WaitNKeys(2):
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	}
}

func TestRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKey()
	d := details(newFakeTrigger(100*time.Millisecond, 500*time.Millisecond))
	d.Properties = Properties(`{"foo":"bar"}`)
	d.Backoff = retry.NewExponentialBackoff(50*time.Millisecond, 800*time.Millisecond, 200*time.Millisecond, 2, 0)
	s.Schedule(ctx, k, d)

	f.F = func(ctx testRunContext) error {
		c := f.Count()
		if c == 1 {
			d2 := d
			d2.Trigger = newFakeTrigger()
			d2.Properties = Properties(`{"baz":"bar"}`)
			s.Schedule(ctx, k, d2)
		}
		if c < 4 {
			return errors.New("test error")
		}
		return nil
	}

	check := func(runCtx testRunContext) error {
		// With the Properties as interface we can no longer check
		// its contents.
		// if !bytes.Equal(runCtx.Properties, d.Properties) {
		// 	 return errors.New("properties mismatch")
		// }
		return nil
	}

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeysCheckContext(check, k, k, k, k):
	}
}

func TestRetryPermanentError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		return retry.Permanent(errors.New("test error"))
	}
	s := NewScheduler[testKey](relativeTime(), f.Run, ll)
	k := randomKey()
	d := details(newFakeTrigger(100 * time.Millisecond))
	d.Backoff = retry.NewExponentialBackoff(50*time.Millisecond, 800*time.Millisecond, 200*time.Millisecond, 2, 0)
	s.Schedule(ctx, k, d)

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-f.Wait(200 * time.Millisecond):
		if c := f.Count(); c != 1 {
			t.Fatal("Unexpected retry")
		}
	}
}

func TestWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	now := timeutc.Now()
	s := NewScheduler[testKey](timeutc.Now, f.Run, ll)
	k := randomKey()
	d := details(newFakeTriggerWithTime(now.Add(50 * time.Millisecond)))
	wdt := func(d time.Duration) WeekdayTime {
		return WeekdayTime{
			Weekday: now.Weekday(),
			Time:    now.Sub(now.Truncate(24*time.Hour)) + d,
		}
	}
	d.Window, _ = NewWindow(wdt(100*time.Millisecond), wdt(200*time.Millisecond), wdt(300*time.Millisecond), wdt(400*time.Millisecond))
	s.Schedule(ctx, k, d)

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-f.Wait(time.Second):
		if c := f.Count(); c != 2 {
			t.Fatal("Run mismatch")
		}
	}
}

type bhv int8

const (
	returnNil bhv = iota
	returnErr
	waitOnCtx
)

func TestWindowWithBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := []bhv{
		returnNil, // and reschedule
		waitOnCtx, // and move to the next window
		returnErr, // and retry in the same window 50ms
		returnErr, // and retry in the same window 100ms
		returnErr, // and reschedule in next window
		returnNil,
	}
	f := newFakeRunner()
	f.F = func(ctx testRunContext) error {
		switch e[f.Count()-1] {
		case returnNil:
			return nil
		case returnErr:
			return errors.New("test error")
		case waitOnCtx:
			select {
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			panic("unsupported behaviour")
		}
	}
	now := timeutc.Now()
	s := NewScheduler[testKey](timeutc.Now, f.Run, ll)
	k := randomKey()
	d := details(newFakeTriggerWithTime(
		now.Add(50*time.Millisecond),
		now.Add(150*time.Millisecond),
		now.Add(1500*time.Millisecond),
	))
	d.Backoff = retry.NewExponentialBackoff(50*time.Millisecond, 800*time.Millisecond, 150*time.Millisecond, 2, 0)

	wdt := func(d time.Duration) WeekdayTime {
		return WeekdayTime{
			Weekday: now.Weekday(),
			Time:    now.Sub(now.Truncate(24*time.Hour)) + d,
		}
	}
	d.Window, _ = NewWindow(
		wdt(10*time.Millisecond), wdt(100*time.Millisecond),
		wdt(200*time.Millisecond), wdt(300*time.Millisecond),
		wdt(400*time.Millisecond), wdt(500*time.Millisecond),
		wdt(600*time.Millisecond), wdt(800*time.Millisecond),
	)
	s.Schedule(ctx, k, d)

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-f.Wait(time.Second):
		if c := f.Count(); c != len(e) {
			t.Fatal("Run mismatch")
		}
	}
}
