// Copyright (C) 2017 ScyllaDB

// +build all integration

package scheduler_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/pkg/schema/table"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/service/scheduler"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/duration"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
)

const (
	mockTask scheduler.TaskType = "mock"

	_interval = 2 * time.Millisecond
	_wait     = 2 * time.Second

	retryTaskWait     = 50 * time.Millisecond
	stopTaskWait      = 10 * time.Millisecond
	startTaskNowSlack = 5 * time.Millisecond

	interval = duration.Duration(10 * retryTaskWait)
)

type mockRunner struct {
	in      chan error
	props   []json.RawMessage
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
	r.props = append(r.props, v)
	r.propsMu.Unlock()
}

func (r *mockRunner) Properties() []json.RawMessage {
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
	session *gocql.Session
	service *scheduler.Service
	runner  *mockRunner

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newSchedTestHelper(t *testing.T, session *gocql.Session) *schedTestHelper {
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task_run")

	scheduler.SetRetryTaskWait(retryTaskWait)
	scheduler.SetStopTaskWait(stopTaskWait)
	scheduler.SetStartTaskNowSlack(startTaskNowSlack)

	s := newTestService(t, session)
	h := &schedTestHelper{
		session: session,
		service: s,
		runner:  newMockRunner(),

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
	s.SetRunner(mockTask, h.runner)

	return h
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

func (h *schedTestHelper) assertNotStatus(ctx context.Context, task *scheduler.Task, s scheduler.Status) {
	h.t.Helper()

	n := int(2 * interval.Duration() / _interval)
	for i := 0; i < n; i++ {
		ok, v := h.getStatus(ctx, task)
		if !ok {
			continue
		}
		if v == s {
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
		ID:        h.taskID,
		Enabled:   true,
		Sched:     s,
	}
}

func newTestService(t *testing.T, session *gocql.Session) *scheduler.Service {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	s, err := scheduler.NewService(
		session,
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServiceScheduleIntegration(t *testing.T) {
	session := CreateSession(t)

	now := func() time.Time {
		return timeutc.Now().Add(2 * startTaskNowSlack)
	}
	future := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

	t.Run("put task once", func(t *testing.T) {
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

		Print("When: another task of the same type is scheduled")
		task.ID = uuid.MustRandom()
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
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
		stmt, names := table.SchedRun.Insert()
		if err := gocqlx.Query(h.session.Query(stmt), names).BindStruct(run).Exec(); err != nil {
			t.Fatal(err)
		}

		Print("And: load tasks")
		if err := h.service.LoadTasks(ctx); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is changed from StatusRunning to status StatusAborted")
		h.assertStatus(ctx, task, scheduler.StatusAborted)

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
		h.runner.Error()
		h.runner.Error()
		h.runner.Error()

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
		h.assertStatus(ctx, task, scheduler.StatusError)
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.assertStatus(ctx, task, scheduler.StatusError)
	})

	t.Run("retry preserve options", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")
		h.runner.Error()
		h.runner.Error()
		h.runner.Error()

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
			return scheduler.Properties([]byte(`{"a":1}`))
		}
		b := func(v scheduler.Properties) scheduler.Properties {
			return scheduler.Properties([]byte(`{"b":1}`))
		}
		if err := h.service.StartTask(ctx, task, a, b); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.assertStatus(ctx, task, scheduler.StatusError)
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("And: properties were preserved")
		if diff := cmp.Diff(h.runner.Properties(), []json.RawMessage{[]byte(`{"b":1}`), []byte(`{"b":1}`)}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("reschedule done", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run ends successfully")
		h.runner.Done()

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

		Print("Given: run will fail twice")
		h.runner.Error()
		h.runner.Error()

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
		h.assertStatus(ctx, task, scheduler.StatusError)
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.assertStatus(ctx, task, scheduler.StatusError)

		Print("Given: run will fail twice")
		h.runner.Error()
		h.runner.Error()

		Print("Then: task is ran two times in next interval")
		h.assertStatus(ctx, task, scheduler.StatusRunning)
		h.assertStatus(ctx, task, scheduler.StatusError)
		h.assertStatus(ctx, task, scheduler.StatusRunning)
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

		Print("Then: task is not executed in future")
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

		Print("And: run ends successfully")
		h.runner.Done()

		Print("Then: task stops with the status done")
		h.assertStatus(ctx, task, scheduler.StatusDone)

		Print("And: task is not executed in future")
		h.assertNotStatus(ctx, task, scheduler.StatusRunning)
	})

}
