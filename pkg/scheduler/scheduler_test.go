// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/go-log"
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
	F func(ctx context.Context, key Key, properties Properties) error
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		c: atomic.NewInt64(0),
		C: make(chan Key),
	}
}

func (r *fakeRunner) Run(ctx context.Context, key Key, properties Properties) error {
	r.c.Inc()
	defer func() {
		r.C <- key
	}()
	if r.F != nil {
		return r.F(ctx, key, properties)
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

func (r *fakeRunner) Count() int64 {
	return r.c.Load()
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

func emptyProperties() Properties {
	return Properties{}
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

func (f fakeTrigger) Next(now time.Time) time.Time {
	for _, t := range f.a {
		if now.Before(t) {
			return t
		}
	}
	return time.Time{}
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
		s.Schedule(ctx, randomKey(), emptyProperties(), newFakeTrigger(0))
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
	s.Schedule(ctx, randomKey(), emptyProperties(), newFakeTrigger(100*time.Millisecond))

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
	f.F = func(ctx context.Context, key Key, properties Properties) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
	k := randomKey()
	s.Schedule(ctx, k, emptyProperties(), newFakeTrigger(50*time.Millisecond))

	time.AfterFunc(100*time.Millisecond, func() {
		s.Schedule(ctx, k, emptyProperties(), newFakeTrigger(300*time.Millisecond))
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
		s.Schedule(ctx, randomKey(), emptyProperties(), newFakeTrigger(100*time.Millisecond))
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
	s.Schedule(ctx, k, emptyProperties(), newFakeTrigger(500*time.Millisecond))

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
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(500*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(600*time.Millisecond))
	s.Schedule(ctx, k[2], emptyProperties(), newFakeTrigger(700*time.Millisecond))

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
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(500*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(600*time.Millisecond))
	s.Schedule(ctx, k[2], emptyProperties(), newFakeTrigger(700*time.Millisecond))

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
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(500*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(600*time.Millisecond))
	s.Schedule(ctx, k[2], emptyProperties(), newFakeTrigger(700*time.Millisecond))

	time.AfterFunc(StartOffset, func() {
		s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(800*time.Millisecond))
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
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(100*time.Millisecond, 300*time.Millisecond, 400*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(200*time.Millisecond))

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
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(500*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(600*time.Millisecond))
	s.Schedule(ctx, k[2], emptyProperties(), newFakeTrigger(700*time.Millisecond))

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
	f.F = func(ctx context.Context, key Key, properties Properties) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := NewScheduler(relativeTime(), f.Run, log.NewDevelopment())
	k := randomKeys(2)
	s.Schedule(ctx, k[0], emptyProperties(), newFakeTrigger(100*time.Millisecond))
	s.Schedule(ctx, k[1], emptyProperties(), newFakeTrigger(200*time.Millisecond))

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
