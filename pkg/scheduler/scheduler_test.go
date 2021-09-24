// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/util/retry"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/atomic"
)

const (
	Timeout     = 1 * time.Second
	StartOffset = 10 * time.Millisecond
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

func startAndWait(ctx context.Context, s *Scheduler) chan struct{} {
	ch := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(ch)
	}()
	return ch
}

type fakeRunner struct {
	c *atomic.Int64
	C chan Key
	F RunFunc
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		c: atomic.NewInt64(0),
		C: make(chan Key),
	}
}

func (r *fakeRunner) Run(ctx RunContext) error {
	r.c.Inc()
	defer func() {
		r.C <- ctx.Key
	}()
	if r.F != nil {
		return r.F(ctx)
	}
	return nil
}

func (r *fakeRunner) WaitKeys(keys ...Key) chan struct{} {
	ch := make(chan struct{})
	go func() {
		for i := 0; i < len(keys); i++ {
			key := <-r.C
			if key != keys[i] {
				msg := fmt.Sprintf("Run key=%s, expected %s", key, keys[i])
				panic(msg)
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

func randomKey() Key {
	return Key(uuid.MustRandom())
}

func randomKeys(n int) []Key {
	keys := make([]Key, n)
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

func TestStopEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFakeRunner()
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())

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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())

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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	f.F = func(ctx RunContext) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())

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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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

func TestStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx RunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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

func TestRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx RunContext) error {
		if f.Count() < 4 {
			return errors.New("test error")
		}
		return nil
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
	k := randomKey()
	d := details(newFakeTrigger(100*time.Millisecond, 500*time.Millisecond))
	d.Backoff = retry.NewExponentialBackoff(50*time.Millisecond, 800*time.Millisecond, 200*time.Millisecond, 2, 0)
	s.Schedule(ctx, k, d)

	select {
	case <-startAndWait(ctx, s):
		t.Fatal("expected a run, scheduler exit")
	case <-time.After(Timeout):
		t.Fatal("expected a run, timeout")
	case <-f.WaitKeys(k, k, k, k, k):
	}
}

func TestRetryPermanentError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFakeRunner()
	f.F = func(ctx RunContext) error {
		return retry.Permanent(errors.New("test error"))
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
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
	f.F = func(ctx RunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	now := timeutc.Now()
	s := NewScheduler(timeutc.Now, f.Run, log.NewDevelopment())
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
	f.F = func(ctx RunContext) error {
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
	s := NewScheduler(timeutc.Now, f.Run, log.NewDevelopment())
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
