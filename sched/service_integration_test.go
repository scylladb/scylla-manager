// Copyright (C) 2017 ScyllaDB

// +build all integration

//go:generate mockgen -destination mock_runner_test.go -mock_names Runner=MockRunner -package sched github.com/scylladb/mermaid/sched/runner Runner

package sched_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/timeutc"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

const (
	mockTask sched.TaskType = "mock"

	_interval = 2 * time.Millisecond
	_wait     = 2 * time.Second

	retryTaskWait       = 50 * time.Millisecond
	taskStartNowSlack   = 5 * time.Millisecond
	monitorTaskInterval = 10 * time.Millisecond

	// use ONLY retries < 5, this should be a safe margin
	interval = sched.Duration(10 * retryTaskWait)
)

type schedTestHelper struct {
	session *gocql.Session
	service *sched.Service
	ctrl    *gomock.Controller
	runner  *sched.MockRunner

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newSchedTestHelper(t *testing.T, session *gocql.Session) *schedTestHelper {
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task")
	ExecStmt(t, session, "TRUNCATE TABLE scheduler_task_run")

	sched.SetRetryTaskWait(retryTaskWait)
	sched.SetTaskStartNowSlack(taskStartNowSlack)
	sched.SetMonitorTaskInterval(monitorTaskInterval)

	s := newTestService(t, session)
	h := &schedTestHelper{
		session: session,
		service: s,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
	h.resetMock()

	return h
}

func (h *schedTestHelper) resetMock() {
	if h.ctrl != nil {
		h.ctrl.Finish()
	}

	h.ctrl = gomock.NewController(h.t)
	h.runner = sched.NewMockRunner(h.ctrl)
	h.service.SetRunner(mockTask, h.runner)
}

func (h *schedTestHelper) assertMockCalled() {
	time.Sleep(5 * retryTaskWait)
	h.resetMock()
}

func (h *schedTestHelper) assertStatus(ctx context.Context, task *sched.Task, s runner.Status) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		runs, err := h.service.GetLastRun(ctx, task, 1)
		if err != nil {
			h.t.Fatal(err)
		}
		if len(runs) == 0 {
			return false
		}
		return runs[0].Status == s
	}, _interval, _wait)
}

func (h *schedTestHelper) close() {
	h.ctrl.Finish()
	h.service.Close()
}

func (h *schedTestHelper) makeTask(s sched.Schedule) *sched.Task {
	return &sched.Task{
		ClusterID: h.clusterID,
		Type:      mockTask,
		ID:        h.taskID,
		Enabled:   true,
		Sched:     s,
	}
}

func newTestService(t *testing.T, session *gocql.Session) *sched.Service {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	s, err := sched.NewService(
		session,
		func(_ context.Context, id uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{
				ID:   id,
				Name: "test_cluster",
			}, nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServiceScheduleIntegration(t *testing.T) {
	session := CreateSession(t)

	now := func() time.Time {
		return timeutc.Now().Add(2 * taskStartNowSlack)
	}

	t.Run("task once", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: a task is scheduled")
		task := h.makeTask(sched.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			t.Fatal(err)
		}
		Print("Then: the task is added")

		task.ID = uuid.MustRandom()
		Print("When: another task task of the same type is scheduled")
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			Print("Then: the task is rejected")
			return
		}
		t.Fatal("two tasks of the same type could be added")
	})

	t.Run("task once update", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: a task is scheduled")
		task := h.makeTask(sched.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			t.Fatal(err)
		}
		Print("Then: the task is added")

		tasks, err := h.service.ListTasks(ctx, h.clusterID, task.Type)
		if err != nil {
			t.Fatal(err)
		}
		cnt1 := len(tasks)
		task.Name = "new name"
		Print("When: the same task is changed and scheduled again")
		if err := h.service.PutTaskOnce(ctx, task); err != nil {
			t.Fatal(err)
		}
		tasks, err = h.service.ListTasks(ctx, h.clusterID, task.Type)
		if err != nil {
			t.Fatal(err)
		}
		cnt2 := len(tasks)
		if cnt1 != cnt2 {
			t.Fatalf("wrong number of tasks after two PutOnce")
		}
		for _, ts := range tasks {
			if ts.ID == task.ID {
				if ts.Name != task.Name {
					t.Fatalf("expected task name %s, got %s", task.Name, ts.Name)
				}
			}
		}
		Print("Then: the task is updated")
	})

	t.Run("task stop", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		m := descriptorMatcher{}
		e := h.runner.EXPECT()
		gomock.InOrder(
			// run
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil).AnyTimes(),
			// stop
			e.Stop(gomock.Any(), m).Return(nil).Return(nil),
			e.Status(gomock.Any(), m).Return(runner.StatusStopping, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusStopped, "", nil),
		)

		Print("When: task is scheduled")
		task := h.makeTask(sched.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, runner.StatusRunning)

		Print("When: task is stopped")
		if err := h.service.StopTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(ctx, task, runner.StatusStopped)
	})

	t.Run("task start", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task is scheduled with start in future")
		task := h.makeTask(sched.Schedule{
			StartDate: time.Unix(1<<60, 0),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: nothing happens")
		h.assertMockCalled()

		Print("Given: task will run")
		m := descriptorMatcher{}
		e := h.runner.EXPECT()
		gomock.InOrder(
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusDone, "", nil),
		)

		Print("When: task is started")
		if err := h.service.StartTask(ctx, task, runner.Opts{}); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(ctx, task, runner.StatusRunning)
		h.assertStatus(ctx, task, runner.StatusDone)
	})

	t.Run("task start error retry", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will fail")
		m := descriptorMatcher{}
		e := h.runner.EXPECT()
		// AnyTimes is used because the test is flaky otherwise
		e.Run(gomock.Any(), m, gomock.Any()).Return(errors.New("test error")).Do(m.set).AnyTimes()

		Print("When: task is scheduled with retry once")
		task := h.makeTask(sched.Schedule{
			StartDate:  now(),
			NumRetries: 1,
			Interval:   interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(ctx, task, runner.StatusStarting)
		h.assertStatus(ctx, task, runner.StatusError)
		h.assertStatus(ctx, task, runner.StatusStarting)
		h.assertStatus(ctx, task, runner.StatusError)
	})

	testReschedule := func(status runner.Status) func(t *testing.T) {
		return func(t *testing.T) {
			h := newSchedTestHelper(t, session)
			defer h.close()
			ctx := context.Background()

			armMock := func() {
				m := descriptorMatcher{}
				e := h.runner.EXPECT()
				gomock.InOrder(
					e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
					e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
					e.Status(gomock.Any(), m).Return(status, "", nil),
				)
			}

			Print("Given: run ends with a given status")
			armMock()

			Print("When: task is scheduled")
			task := h.makeTask(sched.Schedule{
				StartDate: now(),
				Interval:  sched.Duration(10 * retryTaskWait),
			})
			if err := h.service.PutTask(ctx, task); err != nil {
				t.Fatal(err)
			}

			Print("Then: task stops")
			h.assertStatus(ctx, task, runner.StatusRunning)
			h.assertStatus(ctx, task, status)
			h.assertMockCalled()

			Print("Given: run ends with a given status")
			armMock()

			Print("And: task is executed in intervals")
			h.assertStatus(ctx, task, runner.StatusRunning)
			h.assertStatus(ctx, task, status)
			h.assertMockCalled()
		}
	}
	t.Run("done reschedule", testReschedule(runner.StatusDone))
	t.Run("stopped reschedule", testReschedule(runner.StatusStopped))
	t.Run("error reschedule", testReschedule(runner.StatusError))

	t.Run("error retry", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		armMock := func() {
			m := descriptorMatcher{}
			e := h.runner.EXPECT()
			gomock.InOrder(
				// run
				e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
				e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
				e.Status(gomock.Any(), m).Return(runner.StatusError, "", nil),
				// retry
				e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
				e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
				e.Status(gomock.Any(), m).Return(runner.StatusError, "", nil),
			)
		}

		Print("Given: run will fail twice")
		armMock()

		Print("When: task is scheduled with retry once")
		task := h.makeTask(sched.Schedule{
			StartDate:  now(),
			NumRetries: 1,
			Interval:   interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is ran two times")
		h.assertStatus(ctx, task, runner.StatusRunning)
		h.assertStatus(ctx, task, runner.StatusError)
		h.assertStatus(ctx, task, runner.StatusRunning)
		h.assertStatus(ctx, task, runner.StatusError)
		h.assertMockCalled()

		Print("Given: run will fail twice")
		armMock()

		Print("Then: task is ran two times in next interval")
		h.assertStatus(ctx, task, runner.StatusRunning)
		h.assertStatus(ctx, task, runner.StatusError)
		h.assertStatus(ctx, task, runner.StatusRunning)
		h.assertStatus(ctx, task, runner.StatusError)
		h.assertMockCalled()
	})

	t.Run("aborted always retry", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: run will always abort")
		m := descriptorMatcher{}
		e := h.runner.EXPECT()
		gomock.InOrder(
			// run
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusAborted, "", nil),
			// retry
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusAborted, "", nil),
			// retry
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusAborted, "", nil),
			// retry
			e.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(m.set),
			e.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil),
			e.Status(gomock.Any(), m).Return(runner.StatusAborted, "", nil),
		)

		Print("When: task is scheduled with retry once")
		task := h.makeTask(sched.Schedule{
			StartDate:  now(),
			NumRetries: 1,
			Interval:   interval,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is always retried")
		for i := 0; i < 4; i++ {
			h.assertStatus(ctx, task, runner.StatusRunning)
			h.assertStatus(ctx, task, runner.StatusAborted)
		}
	})
}

type descriptorMatcher struct {
	d runner.Descriptor
}

func (m descriptorMatcher) String() string {
	return fmt.Sprint(m.d)
}

func (m descriptorMatcher) Matches(v interface{}) bool {
	if m.d.ClusterID == uuid.Nil {
		return true
	}
	return m.d == v.(runner.Descriptor)
}

func (m descriptorMatcher) set(_, d, _ interface{}) {
	m.d = d.(runner.Descriptor)
}
