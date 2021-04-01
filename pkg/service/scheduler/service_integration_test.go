// Copyright (C) 2017 ScyllaDB

// +build all integration

package scheduler_test

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/store"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
)

const (
	mockTask scheduler.TaskType = "mock"

	_interval = 2 * time.Millisecond
	_wait     = 2 * time.Second

	retryTaskWait = 50 * time.Millisecond
	stopTaskWait  = 10 * time.Millisecond

	interval = duration.Duration(10 * retryTaskWait)
)

type mockRunner struct {
	in      chan error
	props   []scheduler.Properties
	propsMu sync.Mutex
	called  atomic.Int64
}

func newMockRunner() *mockRunner {
	return &mockRunner{
		in: make(chan error, 10),
	}
}

func (r *mockRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	r.called.Inc()
	r.recordProperties(properties)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case v := <-r.in:
		return v
	}
}

func (r *mockRunner) recordProperties(v json.RawMessage) {
	r.propsMu.Lock()
	r.props = append(r.props, scheduler.Properties(v))
	r.propsMu.Unlock()
}

func (r *mockRunner) Properties() []scheduler.Properties {
	r.propsMu.Lock()
	defer r.propsMu.Unlock()
	return r.props
}

func (r *mockRunner) Called() bool {
	return r.called.Load() != 0
}

func (r *mockRunner) Done() {
	select {
	case r.in <- nil:
	default:
		panic("blocked on init")
	}
}

func (r *mockRunner) Error() {
	select {
	case r.in <- errors.New("failed"):
	default:
		panic("blocked on init")
	}
}

type neverEndingRunner struct {
	done chan struct{}
}

func newNeverEndingRunner() *neverEndingRunner {
	return &neverEndingRunner{
		done: make(chan struct{}),
	}
}

func (r *neverEndingRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	select {
	case <-r.done:
		return nil
	}
}

func (r *neverEndingRunner) Stop() {
	close(r.done)
}

type schedTestHelper struct {
	session gocqlx.Session
	service *scheduler.Service
	runner  *mockRunner

	clusterID uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newSchedTestHelper(t *testing.T, session gocqlx.Session) *schedTestHelper {
	ExecStmt(t, session, "TRUNCATE TABLE drawer")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task_run")

	scheduler.SetRetryTaskWait(retryTaskWait)
	scheduler.SetStopTaskWait(stopTaskWait)

	s := newTestService(t, session)
	h := &schedTestHelper{
		session:   session,
		service:   s,
		runner:    newMockRunner(),
		clusterID: uuid.MustRandom(),
		t:         t,
	}
	s.SetRunner(mockTask, h.runner)

	return h
}

func (h *schedTestHelper) assertError(err error, msg string) {
	h.t.Helper()

	if err == nil {
		h.t.Fatalf("Expected error %s, got nil", msg)
	}

	if !regexp.MustCompile(msg).MatchString(err.Error()) {
		h.t.Errorf("Expected error %s, got %s", msg, err.Error())
	} else {
		h.t.Logf("Error message: %s", err.Error())
	}

	h.t.Log("Got error", err)
}

func (h *schedTestHelper) assertStatus(ctx context.Context, task *scheduler.Task, s scheduler.Status) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		ok, v := h.getStatus(ctx, task)
		if !ok {
			return false
		}
		return v == s
	}, _interval, _wait)
}

func (h *schedTestHelper) assertNotStatus(ctx context.Context, task *scheduler.Task, s ...scheduler.Status) {
	h.t.Helper()

	m := strset.New()
	for i := range s {
		m.Add(string(s[i]))
	}

	n := int(2 * interval.Duration() / _interval)
	for i := 0; i < n; i++ {
		ok, v := h.getStatus(ctx, task)
		if !ok {
			continue
		}
		if m.Has(string(v)) {
			h.t.Fatalf("Unexpected status %s", v)
		}
	}
}

func (h *schedTestHelper) getStatus(ctx context.Context, task *scheduler.Task) (ok bool, s scheduler.Status) {
	h.t.Helper()
	runs, err := h.service.GetLastRun(ctx, task, 1)
	if err != nil {
		h.t.Fatal(err)
	}
	if len(runs) == 0 {
		return false, ""
	}
	return true, runs[0].Status
}

func (h *schedTestHelper) close() {
	h.service.Close()
}

func (h *schedTestHelper) makeTask(s scheduler.Schedule) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: h.clusterID,
		Type:      mockTask,
		ID:        uuid.MustRandom(),
		Enabled:   true,
		Sched:     s,
	}
}

func newTestService(t *testing.T, session gocqlx.Session) *scheduler.Service {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	s, err := scheduler.NewService(session, store.NewTableStore(session, table.Drawer), logger)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServiceScheduleIntegration(t *testing.T) {
	session := CreateSession(t)

	now := func() time.Time {
		return timeutc.Now()
	}
	future := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

	t.Run("put task once", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task0 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTaskOnce(ctx, task0); err != nil {
			t.Fatal(err)
		}
		Print("Then: task is added")

		Print("When: another task of the same type is scheduled")
		task1 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTaskOnce(ctx, task1); err != nil {
			Print("Then: the task is rejected")
		} else {
			t.Fatal("two tasks of the same type could be added")
		}
	})

	t.Run("put task once update", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			t.Fatal(err)
		}
		Print("Then: task is added")

		tasks, err := h.service.ListTasks(ctx, h.clusterID, task.Type)
		if err != nil {
			t.Fatal(err)
		}
		total := len(tasks)

		Print("When: task is changed")
		task.Sched.StartDate = time.Unix(1, 0).UTC()
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			t.Fatal(err)
		}
		Print("Then: task is updated")

		tasks, err = h.service.ListTasks(ctx, h.clusterID, task.Type)
		if err != nil {
			t.Fatal(err)
		}
		if total != len(tasks) {
			t.Fatalf("Wrong number of tasks after two PutOnce")
		}
		for _, ts := range tasks {
			if ts.ID == task.ID {
				if ts.Sched != task.Sched {
					t.Fatalf("Expected task %+v, got %+v", task.Sched, ts.Sched)
				}
			}
		}
	})

	t.Run("get last run with status", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})

		Print("And: task runs are created")
		putRun := func(status scheduler.Status) *scheduler.Run {
			r := task.NewRun()
			r.Status = status
			if err := table.SchedRun.InsertQuery(session).BindStruct(r).ExecRelease(); err != nil {
				t.Fatal(err)
			}
			return r
		}
		run := putRun(scheduler.StatusDone)
		putRun(scheduler.StatusError)
		putRun(scheduler.StatusError)
		putRun(scheduler.StatusRunning)

		Print("Then: last run is returned")
		v, err := h.service.GetLastRunWithStatus(task, scheduler.StatusDone)
		if err != nil {
			t.Fatal("GetLastRunWithStatus() error", err)
		}
		if diff := cmp.Diff(run, v, UUIDComparer(), cmpopts.IgnoreTypes(time.Time{})); diff != "" {
			t.Fatalf("GetLastRunWithStatus() = %v, expected %v, diff\n%s", v, run, diff)
		}
	})

	t.Run("load tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("When: runs are left in StatusRunning")
		run := task.NewRun()
		run.Status = scheduler.StatusRunning
		if err := table.SchedRun.InsertQuery(h.session).BindStruct(run).Exec(); err != nil {
			t.Fatal(err)
		}

		Print("And: load tasks")
		if err := h.service.LoadTasks(ctx); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is changed from StatusRunning to status StatusAborted")
		h.assertStatus(ctx, task, scheduler.StatusAborted)
		Print("And: end time is set")
		runs, err := h.service.GetLastRun(ctx, task, 1)
		if err != nil {
			h.t.Fatal(err)
		}
		if runs[0].EndTime == nil {
			t.Fatal("Expected end time got nil")
		}

		Print("And: aborted tasks are immediately resumed")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("stop task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task is stopped")
		if err := h.service.StopTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(ctx, task, scheduler.StatusStopped)
	})

	t.Run("stop not responding tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: never ending task is scheduled")
		h.service.SetRunner(mockTask, newNeverEndingRunner())

		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task is stopped")
		if err := h.service.StopTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(ctx, task, scheduler.StatusError)
	})

	t.Run("service close aborts tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: tasks are running")
		t0 := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, t0); err != nil {
			t.Fatal(err)
		}

		t1 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, t1); err != nil {
			t.Fatal(err)
		}
		if err := h.service.StartTask(ctx, t1); err != nil {
			t.Fatal(err)
		}

		h.assertStatus(ctx, t0, scheduler.StatusRunning)
		h.assertStatus(ctx, t1, scheduler.StatusRunning)

		Print("When: service is closed")
		h.service.Close()

		Print("Then: tasks are aborted")
		h.assertStatus(ctx, t0, scheduler.StatusAborted)
		h.assertStatus(ctx, t1, scheduler.StatusAborted)
	})

	t.Run("start task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled with start in future")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: task is started")
		if err := h.service.StartTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task run finishes")
		h.runner.Done()

		Print("Then: task status is StatusDone")
		h.assertStatus(ctx, task, scheduler.StatusDone)
	})

	t.Run("start running task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled with start in future")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: task is started")
		if err := h.service.StartTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("And: task is started while it's running")
		if err := h.service.StartTask(ctx, task); err == nil {
			t.Fatal("Starting a running task should error")
		}

		Print("Then: task status is StatusRunning")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task run finishes")
		h.runner.Done()

		Print("Then: task status is StatusDone")
		h.assertStatus(ctx, task, scheduler.StatusDone)
	})

	t.Run("retry", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")

		Print("When: task is scheduled with retry once")
		task := h.makeTask(scheduler.Schedule{
			StartDate:  now(),
			NumRetries: 1,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("retry preserve options", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")

		Print("When: task is scheduled with retry once")
		task := h.makeTask(scheduler.Schedule{
			StartDate:  future,
			NumRetries: 1,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: task is started with additional properties")
		a := func(v scheduler.Properties) scheduler.Properties {
			return scheduler.Properties(`{"a":1}`)
		}
		b := func(v scheduler.Properties) scheduler.Properties {
			return scheduler.Properties(`{"b":1}`)
		}
		if err := h.service.StartTask(ctx, task, a, b); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times and properties were preserved")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)

		Print("And: properties are preserved")
		if diff := cmp.Diff(h.runner.Properties(), []scheduler.Properties{
			scheduler.Properties(`{"b":1}`),
			scheduler.Properties(`{"b":1}`),
		}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("reschedule done", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run ends successfully")

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
			Interval:  interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops with the status")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Done()
		h.assertStatus(ctx, task, scheduler.StatusDone)

		Print("And: task is executed in intervals")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("reschedule stopped", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
			Interval:  duration.Duration(10 * retryTaskWait),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task is stopped")
		if err := h.service.StopTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(ctx, task, scheduler.StatusStopped)

		Print("And: task is executed in intervals")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("reschedule error", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")

		Print("When: task is scheduled with retry once")
		task := h.makeTask(scheduler.Schedule{
			StartDate:  now(),
			NumRetries: 1,
			Interval:   interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("Then: task is ran two times in next interval")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)
	})

	t.Run("stop and disable task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
			Interval:  interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task is stopped")
		if err := h.service.StopTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(ctx, task, scheduler.StatusStopped)

		Print("When: task is disabled")
		task.Enabled = false
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("disable running task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
			Interval:  interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task is disabled")
		task.Enabled = false
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task continues to run")
		h.assertNotStatus(ctx, task, scheduler.StatusStopped)

		Print("When: run ends successfully")
		h.runner.Done()

		Print("Then: task stops with the status done")
		h.assertStatus(ctx, task, scheduler.StatusDone)

		Print("And: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("update schedule of running task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: one shot task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task schedule is updated with interval")
		task.Sched = scheduler.Schedule{
			StartDate: now(),
			Interval:  interval,
		}
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task continues to run")
		h.assertNotStatus(ctx, task, scheduler.StatusStopped)

		Print("And: the other task is not started")
		if n := h.runner.called.Load(); n != 1 {
			t.Fatalf("Runner executed %d times", n)
		}

		Print("When: run ends successfully")
		h.runner.Done()

		Print("Then: task stops with the status done")
		h.assertStatus(ctx, task, scheduler.StatusDone)
		Print("And: starts again")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("update schedule of not running task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: one shot task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: task schedule is updated with interval")
		task.Sched = scheduler.Schedule{
			StartDate: now(),
		}
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("global task options", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		props := []scheduler.Properties{
			scheduler.Properties(`{"a":1}`),
			scheduler.Properties(`{"b":1}`),
		}
		pos := atomic.NewInt32(-1)

		Print("Given: global task opts")
		h.service.SetTaskOpt(func(ctx context.Context, task scheduler.Task) (scheduler.Properties, error) {
			return props[pos.Inc()], nil
		})

		Print("When: task is scheduled with retry once")
		task := h.makeTask(scheduler.Schedule{
			StartDate:  now(),
			NumRetries: 1,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times and properties were preserved")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)

		Print("And: properties are preserved")
		if diff := cmp.Diff(h.runner.Properties(), props); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("suspend and resume", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task0 is scheduled now")
		task0 := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 runs")
		h.assertStatus(ctx, task0, scheduler.StatusRunning)

		Print("When: task1 is scheduled in future")
		task1 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}

		Print("Then: task1 is not executed")
		h.assertNotStatus(ctx, task1, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: task0 status is StatusStopped")
		h.assertStatus(ctx, task0, scheduler.StatusStopped)

		Print("And: task0 cannot be started")
		h.assertError(h.service.StartTask(ctx, task0), "suspended")

		Print("And: task1 cannot be started")
		h.assertError(h.service.StartTask(ctx, task0), "suspended")

		Print("When: scheduler is resumed with start tasks option")
		if err := h.service.Resume(ctx, h.clusterID, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports not suspended status")
		if h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected not suspended")
		}

		Print("And: task0 status is StatusRunning")
		h.assertStatus(ctx, task0, scheduler.StatusRunning)

		Print("And: task1 is not executed")
		h.assertNotStatus(ctx, task1, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 status is StatusStopped")
		h.assertStatus(ctx, task0, scheduler.StatusStopped)

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 is not executed")
		h.assertNotStatus(ctx, task0, scheduler.StatusRunning)

		Print("And: task1 is not executed")
		h.assertNotStatus(ctx, task1, scheduler.StatusRunning)

		Print("When: scheduler is resumed again it fails")
		if err := h.service.Resume(ctx, h.clusterID, false); err == nil {
			t.Fatal("Resume(), expected error")
		} else {
			t.Log("Resume() error", err)
		}
	})

	t.Run("put task when suspended", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("When: task is scheduled in future")
		task0 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		task0.ID = uuid.Nil
		Print("Then: task is added")
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("When: task is scheduled now")
		task1 := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		task1.ID = uuid.Nil
		Print("Then: task is rejected")
		h.assertError(h.service.PutTask(ctx, task1), "suspended")

		Print("When: health check task is scheduled now")
		task2 := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		task2.Type = scheduler.HealthCheckRESTTask
		task2.ID = uuid.Nil
		Print("Then: task is added")
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("load tasks when suspended", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled now")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is StatusStopped")
		h.assertStatus(ctx, task, scheduler.StatusStopped)

		Print("When: service is restarted")
		h.service.Close()
		h.service = newTestService(t, session)
		h.service.SetRunner(mockTask, h.runner)

		Print("And: load tasks")
		if err := h.service.LoadTasks(ctx); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)

		Print("When: scheduler is resumed with start tasks option")
		if err := h.service.Resume(ctx, h.clusterID, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is StatusRunning")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
	})

	t.Run("suspend issue 2496", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		wait := time.Second

		Print("When: task0 is scheduled in a second")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now().Add(wait),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: scheduler is suspended during the start time")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)

		Print("When: task start date passes by")
		time.Sleep(wait)

		Print("And: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)
	})
}
