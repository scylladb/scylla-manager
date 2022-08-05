// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package scheduler_test

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	_interval = 10 * time.Millisecond
	_wait     = 2 * time.Second

	mockTask scheduler.TaskType = "mock"
	interval                    = duration.Duration(100 * time.Millisecond)
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

type schedulerTestHelper struct {
	session gocqlx.Session
	service *scheduler.Service
	runner  *mockRunner

	clusterID uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newSchedTestHelper(t *testing.T, session gocqlx.Session) *schedulerTestHelper {
	ExecStmt(t, session, "TRUNCATE TABLE drawer")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task_run")

	s := newTestService(session)
	h := &schedulerTestHelper{
		session:   session,
		service:   s,
		runner:    newMockRunner(),
		clusterID: uuid.MustRandom(),
		t:         t,
	}
	s.SetRunner(mockTask, h.runner)

	return h
}

func (h *schedulerTestHelper) assertError(err error, msg string) {
	h.t.Helper()

	if err == nil {
		h.t.Fatalf("Expected error %s, got nil", msg)
	}

	if !regexp.MustCompile(msg).MatchString(err.Error()) {
		h.t.Errorf("Expected error %s, got %s", msg, err.Error())
	} else {
		h.t.Logf("Error message: %s", err.Error())
	}
}

func (h *schedulerTestHelper) assertStatus(task *scheduler.Task, s scheduler.Status) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		v := h.getStatus(task)
		return v == s
	}, _interval, _wait)
}

func (h *schedulerTestHelper) assertNotStatus(task *scheduler.Task, s ...scheduler.Status) {
	h.t.Helper()

	m := strset.New()
	for i := range s {
		m.Add(string(s[i]))
	}

	n := int(2 * interval.Duration() / _interval)
	for i := 0; i < n; i++ {
		v := h.getStatus(task)
		if v == "" {
			continue
		}
		if m.Has(string(v)) {
			h.t.Fatalf("Unexpected status %s", s)
		}
	}
}

func (h *schedulerTestHelper) getStatus(task *scheduler.Task) scheduler.Status {
	h.t.Helper()
	r, err := h.service.GetLastRun(task)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			return ""
		}
		h.t.Fatal(err)
	}
	return r.Status
}

func (h *schedulerTestHelper) close() {
	h.service.Close()
}

func (h *schedulerTestHelper) makeTaskWithStartDate(s time.Time) *scheduler.Task {
	return h.makeTask(scheduler.Schedule{StartDate: s})
}

func (h *schedulerTestHelper) makeTask(s scheduler.Schedule) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: h.clusterID,
		Type:      mockTask,
		ID:        uuid.MustRandom(),
		Enabled:   true,
		Sched:     s,
	}
}

func newTestService(session gocqlx.Session) *scheduler.Service {
	s, _ := scheduler.NewService(
		session,
		metrics.NewSchedulerMetrics(),
		store.NewTableStore(session, table.Drawer),
		log.NewDevelopmentWithLevel(zapcore.DebugLevel),
	)
	return s
}

const emptyStatus scheduler.Status = ""

func TestServiceScheduleIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	never := time.Time{}
	future := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	now := func() time.Time {
		return timeutc.Now().Add(100 * time.Millisecond)
	}

	t.Run("get nth last run", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: 2 task runs")
		task := h.makeTaskWithStartDate(future)
		run1 := task.NewRun()
		run1.Status = scheduler.StatusDone
		if err := h.service.PutTestRun(run1); err != nil {
			t.Fatal(err)
		}
		run0 := task.NewRun()
		run0.Status = scheduler.StatusRunning
		if err := h.service.PutTestRun(run0); err != nil {
			t.Fatal(err)
		}

		if r, err := h.service.GetNthLastRun(ctx, task, 1); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(r, run1, UUIDComparer(), cmpopts.IgnoreFields(scheduler.Run{}, "StartTime")); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("put task name conflict", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task0 := h.makeTaskWithStartDate(future)
		task0.Name = "foo"
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}
		Print("Then: task is added")

		Print("When: another task of the same type with the same name is scheduled")
		task1 := h.makeTaskWithStartDate(future)
		task1.Name = "foo"
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Log(err)
			Print("Then: the task is rejected")
		} else {
			t.Fatal("two tasks of the same type and name could be added")
		}
	})

	t.Run("load tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: two tasks are scheduled in future")
		task0 := h.makeTaskWithStartDate(future)
		if err := h.service.PutTestTask(task0); err != nil {
			t.Fatal(err)
		}
		task1 := h.makeTaskWithStartDate(future)
		if err := h.service.PutTestTask(task1); err != nil {
			t.Fatal(err)
		}

		Print("And: one of them is in status RUNNING")
		run := task0.NewRun()
		run.Status = scheduler.StatusRunning
		if err := h.service.PutTestRun(run); err != nil {
			t.Fatal(err)
		}

		Print("And: there is one disabled task")
		task2 := h.makeTaskWithStartDate(never)
		task2.Enabled = false
		if err := h.service.PutTestTask(task2); err != nil {
			t.Fatal(err)
		}

		Print("When: load tasks")
		if err := h.service.LoadTasks(ctx); err != nil {
			t.Fatal(err)
		}

		Print("Then: RUNNING tasks are immediately resumed")
		h.assertStatus(task0, scheduler.StatusRunning)
		h.assertStatus(task1, emptyStatus)
		h.assertStatus(task2, emptyStatus)
	})

	t.Run("stop task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled")
		task := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task is stopped")
		h.service.StopTask(ctx, task)

		Print("Then: task status is STOPPED")
		h.assertStatus(task, scheduler.StatusStopped)
	})

	t.Run("service close aborts tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: tasks are running")
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}
		task1 := h.makeTaskWithStartDate(future)
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}
		h.service.StartTask(ctx, task1)
		h.assertStatus(task0, scheduler.StatusRunning)
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: service is closed")
		h.service.Close()

		Print("Then: tasks are aborted")
		h.assertStatus(task0, scheduler.StatusAborted)
		h.assertStatus(task1, scheduler.StatusAborted)
	})

	t.Run("task status", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: task scheduled now")
		task := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: task run")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task finish")
		h.runner.Done()

		Print("Then: task status is StatusDone")
		h.assertStatus(task, scheduler.StatusDone)

		assertTaskStatusInfo := func(status scheduler.Status, successCount, errorCount int, lastSuccess, lastError bool) {
			t.Helper()

			var v *scheduler.Task
			WaitCond(h.t, func() bool {
				var err error
				v, err = h.service.GetTaskByID(ctx, task.ClusterID, task.Type, task.ID)
				if err != nil {
					t.Fatal(err)
				}
				return v.Status == status
			}, _interval, _wait)

			if v.SuccessCount != successCount {
				t.Fatalf("SuccessCount=%d, expected %d", v.SuccessCount, successCount)
			}
			if v.ErrorCount != errorCount {
				t.Fatalf("ErrorCount=%d, expected %d", v.ErrorCount, errorCount)
			}
			if (v.LastSuccess != nil) != lastSuccess {
				t.Fatalf("LastSuccess=%s, expected %v", v.LastSuccess, lastSuccess)
			}
			if (v.LastError != nil) != lastError {
				t.Fatalf("LastSuccess=%s, expected %v", v.LastError, lastError)
			}
		}

		Print("And: task status information is persisted")
		assertTaskStatusInfo(scheduler.StatusDone, 1, 0, true, false)

		Print("When: task is started")
		h.service.StartTask(ctx, task)

		Print("Then: task run")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task finish")
		h.runner.Done()

		Print("Then: task status information is persisted")
		assertTaskStatusInfo(scheduler.StatusDone, 2, 0, true, false)

		Print("When: task is started")
		h.service.StartTask(ctx, task)

		Print("Then: task run")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task finish")
		h.runner.Error()

		Print("Then: task status information is persisted")
		assertTaskStatusInfo(scheduler.StatusError, 2, 1, true, true)
	})

	t.Run("start task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: task scheduled in future")
		task0 := h.makeTaskWithStartDate(future)
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}
		Print("Given: task scheduled never")
		task1 := h.makeTaskWithStartDate(never)
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}

		Print("When: tasks are started")
		h.service.StartTask(ctx, task0)
		h.service.StartTask(ctx, task1)

		Print("Then: tasks run")
		h.assertStatus(task0, scheduler.StatusRunning)
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: tasks finish")
		h.runner.Done()
		h.runner.Done()

		Print("Then: task status is StatusDone")
		h.assertStatus(task0, scheduler.StatusDone)
		h.assertStatus(task1, scheduler.StatusDone)
	})

	t.Run("start task no continue", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: task scheduled in future")
		task := h.makeTaskWithStartDate(future)
		task.Sched.NumRetries = 1
		task.Sched.RetryWait = duration.Duration(10 * time.Millisecond)
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		props := []json.RawMessage{
			json.RawMessage(`{"continue":false}`),
			json.RawMessage(`{}`),
		}

		Print("When: task is started")
		h.service.StartTaskNoContinue(ctx, task)

		Print("Then: task is ran two times")
		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)

		Print("And: properties are preserved")
		if diff := cmp.Diff(h.runner.Properties(), props); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("retry", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")

		Print("When: task is scheduled with retry once")
		task := h.makeTaskWithStartDate(now())
		task.Sched.NumRetries = 1
		task.Sched.RetryWait = duration.Duration(10 * time.Millisecond)
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)
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
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task is stopped")
		h.service.StopTask(ctx, task)

		Print("Then: task stops")
		h.assertStatus(task, scheduler.StatusStopped)

		Print("When: task is disabled")
		task.Enabled = false
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)
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
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task is disabled")
		task.Enabled = false
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task continues to run")
		h.assertNotStatus(task, scheduler.StatusStopped)

		Print("When: run ends successfully")
		h.runner.Done()

		Print("Then: task stops with the status done")
		h.assertStatus(task, scheduler.StatusDone)

		Print("And: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)
	})

	t.Run("decorate task properties", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		props := []json.RawMessage{
			json.RawMessage(`{"a":1}`),
			json.RawMessage(`{"b":1}`),
		}
		pos := atomic.NewInt32(-1)

		Print("Given: properties decorators")
		h.service.SetPropertiesDecorator(mockTask, func(ctx context.Context, clusterID, taskID uuid.UUID, properties json.RawMessage) (json.RawMessage, error) {
			return props[pos.Inc()], nil
		})

		Print("When: task is scheduled with retry once")
		task := h.makeTask(scheduler.Schedule{
			StartDate:  now(),
			NumRetries: 1,
			RetryWait:  duration.Duration(10 * time.Millisecond),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times and properties were preserved")
		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task, scheduler.StatusError)

		Print("And: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)

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
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 runs")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: task1 is scheduled in future")
		task1 := h.makeTask(scheduler.Schedule{
			StartDate: future,
		})
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}

		Print("Then: task1 is not executed")
		h.assertNotStatus(task1, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: task0 status is StatusStopped")
		h.assertStatus(task0, scheduler.StatusStopped)

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
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("And: task1 is not executed")
		h.assertNotStatus(task1, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 status is StatusStopped")
		h.assertStatus(task0, scheduler.StatusStopped)

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task1 is not executed")
		h.assertNotStatus(task1, scheduler.StatusRunning)
	})

	t.Run("suspend task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled now")
		task := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		p, _ := json.Marshal(scheduler.SuspendProperties{
			Duration:   duration.Duration(time.Second),
			StartTasks: true,
		})
		suspendTask := &scheduler.Task{
			ClusterID:  h.clusterID,
			Type:       scheduler.SuspendTask,
			Enabled:    true,
			Properties: p,
		}

		Print("When: suspend task is scheduled")
		if err := h.service.PutTask(ctx, suspendTask); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is StatusStopped")
		h.assertStatus(task, scheduler.StatusStopped)

		Print("And: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: task is automatically resumed")
		h.assertStatus(task, scheduler.StatusRunning)
		h.runner.Done()
		h.assertStatus(task, scheduler.StatusDone)
	})

	t.Run("put task when suspended", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("When: new task is scheduled in future")
		task0 := h.makeTaskWithStartDate(future)
		task0.ID = uuid.Nil
		Print("Then: task is rejected")
		h.assertError(h.service.PutTask(ctx, task0), "suspended")

		Print("When: task is updated")
		task1 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}
		Print("Then: task is not executed")
		h.assertNotStatus(task1, scheduler.StatusRunning)
	})

	t.Run("load tasks when suspended", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled now")
		task := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is StatusStopped")
		h.assertStatus(task, scheduler.StatusStopped)

		Print("When: service is restarted")
		h.service.Close()
		h.service = newTestService(session)
		h.service.SetRunner(mockTask, h.runner)

		Print("And: load tasks")
		if err := h.service.LoadTasks(ctx); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)

		Print("When: scheduler is resumed with start tasks option")
		if err := h.service.Resume(ctx, h.clusterID, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is StatusRunning")
		h.assertStatus(task, scheduler.StatusRunning)
	})

	t.Run("suspend issue 2496", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		wait := time.Second

		Print("When: task0 is scheduled in a second")
		task := h.makeTaskWithStartDate(now().Add(wait / 2))
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("And: scheduler is suspended during the start time")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)

		Print("When: task start date passes by")
		time.Sleep(wait)

		Print("And: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)
	})

	t.Run("suspend issue 2849", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: suspending it again has no side effects")
		if err := h.service.Suspend(ctx, h.clusterID); err != nil {
			t.Fatal(err)
		}

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports not suspended status")
		if h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected resumed")
		}
	})
}
