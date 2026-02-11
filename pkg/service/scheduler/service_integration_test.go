// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package scheduler_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	sched "github.com/scylladb/scylla-manager/v3/pkg/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/healthcheck"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
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

// overrideCtxRunner is a wrapper around mockRunner which overrides
// task execution ctx. Such ctx is not canceled when task is stopped,
// but can be canceled manually by calling Cancel method.
type overrideCtxRunner struct {
	*mockRunner

	mu        sync.Mutex
	cancelCtx map[uuid.UUID]context.CancelFunc
}

func newOverrideCtxRunner() *overrideCtxRunner {
	return &overrideCtxRunner{
		mockRunner: newMockRunner(),
		cancelCtx:  make(map[uuid.UUID]context.CancelFunc),
	}
}

func (r *overrideCtxRunner) Run(_ context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	r.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelCtx[taskID] = cancel
	r.mu.Unlock()

	return r.mockRunner.Run(ctx, clusterID, taskID, runID, properties)
}

func (r *overrideCtxRunner) Cancel(taskID uuid.UUID) {
	r.mu.Lock()
	if cancel := r.cancelCtx[taskID]; cancel != nil {
		cancel()
	}
	delete(r.cancelCtx, taskID)
	r.mu.Unlock()
}

type schedulerTestHelper struct {
	session gocqlx.Session
	client  *scyllaclient.Client
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

	c := scyllaclient.TestConfig(testconfig.ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(c, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	s := newTestService(session)
	h := &schedulerTestHelper{
		session:   session,
		client:    client,
		service:   s,
		runner:    newMockRunner(),
		clusterID: uuid.MustRandom(),
		t:         t,
	}
	s.SetRunner(mockTask, h.runner)
	s.SetRunner(scheduler.HealthCheckTask, h.runner)
	s.SetRunner(scheduler.BackupTask, h.runner)
	s.SetRunner(scheduler.RepairTask, h.runner)
	s.SetRunner(scheduler.TabletRepairTask, h.runner)
	s.SetRunner(scheduler.RestoreTask, h.runner)
	s.SetRunner(scheduler.One2OneRestoreTask, h.runner)

	backupSvc, err := backup.NewService(
		session,
		backup.Config{},
		metrics.NewBackupMetrics(),
		func(context.Context, uuid.UUID) (string, error) {
			return "", errors.New("not implemented")
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(context.Context, uuid.UUID, ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return gocqlx.Session{}, errors.New("not implemented")
		},
		func(context.Context, uuid.UUID, string) (*dynamodb.Client, error) {
			return nil, errors.New("not implemented")
		},
		nil,
		log.NewDevelopment(),
	)
	if err != nil {
		t.Fatal(err)
	}

	s.SetTaskCleaner(scheduler.BackupTask, backupSvc.DeleteLocalSnapshots)

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

func (h *schedulerTestHelper) assertStatus(task *scheduler.Task, s ...scheduler.Status) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		v := h.getStatus(task)
		return slices.Contains(s, v)
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
		if errors.Is(err, util.ErrNotFound) {
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

func (h *schedulerTestHelper) makeTaskOfTypeWithStartDate(tp scheduler.TaskType, s time.Time) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: h.clusterID,
		Type:      tp,
		ID:        uuid.MustRandom(),
		Enabled:   true,
		Sched:     scheduler.Schedule{StartDate: s},
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

	t.Run("update healthcheck tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: default healthcheck tasks in SM DB")
		tasks := []*scheduler.Task{
			{
				ClusterID: h.clusterID,
				Type:      scheduler.HealthCheckTask,
				Enabled:   true,
				Name:      "cql",
				Sched: scheduler.Schedule{
					Cron: healthcheck.DefaultConfig().CQLPingCron,
				},
				Properties: json.RawMessage(`{"mode": "cql"}`),
			},
			{
				ClusterID: h.clusterID,
				Type:      scheduler.HealthCheckTask,
				Enabled:   true,
				Name:      "rest",
				Sched: scheduler.Schedule{
					Cron: healthcheck.DefaultConfig().RESTPingCron,
				},
				Properties: json.RawMessage(`{"mode": "rest"}`),
			},
			{
				ClusterID: h.clusterID,
				Type:      scheduler.HealthCheckTask,
				Enabled:   true,
				Name:      "alternator",
				Sched: scheduler.Schedule{
					Cron: healthcheck.DefaultConfig().AlternatorPingCron,
				},
				Properties: json.RawMessage(`{"mode": "alternator"}`),
			},
		}
		for _, task := range tasks {
			if err := h.service.PutTask(ctx, task); err != nil {
				t.Fatal(err)
			}
		}

		Print("And: their IDs")
		nameToID := make(map[string]uuid.UUID)
		listed, err := h.service.ListTasks(ctx, h.clusterID, scheduler.ListFilter{})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range listed {
			nameToID[task.Name] = task.ID
		}

		Print("When: update healthcheck tasks with new config")
		nameToSpec := map[string]string{
			"cql":        "0 * * * * *",
			"rest":       "0 0 * * * *",
			"alternator": "0 0 0 * * *",
		}
		cfg := healthcheck.Config{
			CQLPingCron:        schedules.MustCron(nameToSpec["cql"], time.Time{}),
			RESTPingCron:       schedules.MustCron(nameToSpec["rest"], time.Time{}),
			AlternatorPingCron: schedules.MustCron(nameToSpec["alternator"], time.Time{}),
		}
		if err := h.service.UpdateHealthcheckTasks(ctx, cfg); err != nil {
			t.Fatal(err)
		}

		Print("Then: healthcheck tasks are updated")
		updated, err := h.service.ListTasks(ctx, h.clusterID, scheduler.ListFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(updated) != len(listed) {
			t.Fatalf("Expected %d healthcheck tasks, got %d", len(listed), len(updated))
		}
		for _, task := range updated {
			if nameToID[task.Name] != task.ID {
				t.Fatalf("Healthcheck task %s ID %s, expected: %s", task.Name, task.ID, nameToID[task.Name])
			}
			if nameToSpec[task.Name] != task.Sched.Cron.Spec {
				t.Fatalf("Healthcheck task %s cron spec %q: expected %q", task.Name, task.Sched.Cron.Spec, nameToSpec[task.Name])
			}
		}

		// Checks for #4599
		Print("When: delete one healthcheck task")
		if err := h.service.DeleteTask(ctx, tasks[0]); err != nil {
			t.Fatal(err)
		}

		Print("Then: update healthcheck tasks succeeds")
		if err := h.service.UpdateHealthcheckTasks(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("stop task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := t.Context()

		runner := newOverrideCtxRunner()
		h.service.SetRunner(mockTask, runner)

		Print("When: task is scheduled with overridden ctx")
		task := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}
		defer runner.Cancel(task.ID)

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task is stopped, yet it continues running on overridden ctx")
		if err := h.service.StopTask(ctx, task, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task status is STOPPING")
		h.assertStatus(task, scheduler.StatusStopping)
		dbTask, err := h.service.GetTaskByID(ctx, task.ClusterID, task.Type, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if dbTask.Status != scheduler.StatusStopping {
			t.Fatalf("Expected task status: %s, got: %s", scheduler.StatusStopping, dbTask.Status)
		}

		Print("When: task overridden ctx is canceled")
		runner.Cancel(task.ID)

		Print("Then: task status is STOPPED")
		h.assertStatus(task, scheduler.StatusStopped)
		dbTask, err = h.service.GetTaskByID(ctx, task.ClusterID, task.Type, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if dbTask.Status != scheduler.StatusStopped {
			t.Fatalf("Expected task status: %s, got: %s", scheduler.StatusStopped, dbTask.Status)
		}
	})

	t.Run("stop task doesn't deadlock", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := t.Context()

		runner := newOverrideCtxRunner()
		h.service.SetRunner(mockTask, runner)

		const (
			taskCnt       = 10
			taskRunCnt    = 50
			cancelStopCnt = 5
		)
		tasks := make([]*scheduler.Task, taskCnt)
		for i := range tasks {
			// Start date in the future so that the tasks are not started on their own
			tasks[i] = h.makeTaskWithStartDate(now().Add(time.Hour))
			if err := h.service.PutTask(ctx, tasks[i]); err != nil {
				t.Error(err)
			}
			// Ensure that task ctx is canceled,
			// so that it doesn't block scheduler on closing.
			defer runner.Cancel(tasks[i].ID)
		}

		randSleep := func(maxMs int) {
			time.Sleep(time.Duration(rand.Intn(maxMs+1)) * time.Millisecond)
		}
		// The general idea is to start/end/stop tasks in random order
		// to check if scheduler handles such scenarios well.
		wg := sync.WaitGroup{}
		for _, task := range tasks {
			wg.Go(func() {
				for range taskRunCnt {
					if err := h.service.StartTask(ctx, task); err != nil {
						t.Error(err)
					}
					h.assertStatus(task, scheduler.StatusRunning)

					randSleep(5)
					// Canceling task ctx directly mimics task finishing naturally with error.
					// We randomly stop and cancel task ctx in different orders with possible
					// small sleeps in between to verify that there are no races between task
					// finishing naturally and task being stopped. Sleep duration was chosen
					// empirically to have around 50/50% ratio of tasks finishing with
					// scheduler.StatusStopped and scheduler.StatusError.
					nestedWG := sync.WaitGroup{}
					for range cancelStopCnt {
						nestedWG.Go(func() {
							randSleep(1)
							if err := h.service.StopTask(ctx, task, false); err != nil {
								t.Error(err)
							}
						})
						nestedWG.Go(func() {
							randSleep(5)
							runner.Cancel(task.ID)
						})
					}
					nestedWG.Wait()

					h.assertStatus(task, scheduler.StatusStopped, scheduler.StatusError)
				}
			})
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("Scheduling didn't finish within expected timeout")
		}
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
		h.service.SetTaskNoContinue(task.ID, false)
		h.service.StartTask(ctx, task)

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

	t.Run("start task no continue force", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: task scheduled in future")
		task := h.makeTaskWithStartDate(now().Add(time.Second))
		task.Sched.NumRetries = 1
		task.Sched.RetryWait = duration.Duration(10 * time.Millisecond)
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		props := []json.RawMessage{
			json.RawMessage(`{"continue":false}`),
			json.RawMessage(`{}`),
		}

		Print("When: task is started on its own")
		h.service.SetTaskNoContinue(task.ID, true)

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
		// Remove ID to create task instead of updating it.
		// On task update, the 'deleted' column is not set,
		// hence it equals null and messes up task listing.
		task.ID = uuid.Nil
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task is stopped and disabled")
		if err := h.service.StopTask(ctx, task, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: task stops")
		h.assertStatus(task, scheduler.StatusStopped)

		tasks, err := h.service.ListTasks(ctx, h.clusterID, scheduler.ListFilter{Disabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 {
			t.Fatalf("Expected 1 task, got %d", len(tasks))
		}

		Print("Then: task has not next activation scheduled")
		if tasks[0].NextActivation != nil {
			t.Fatalf("Expected NextActivation to be nil, got %v", tasks[0].NextActivation)
		}
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

	t.Run("decorate backup task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		type retentionProp struct {
			Retention     int                 `json:"retention"`
			RetentionDays int                 `json:"retention_days"`
			RetentionMap  backup.RetentionMap `json:"retention_map"`
		}

		taskRetentions := []retentionProp{
			{
				Retention:     3,
				RetentionDays: 30,
			},
			{
				Retention:     2,
				RetentionDays: 14,
			},
			{
				Retention:     11,
				RetentionDays: 100,
			},
		}

		newTask := func(rp retentionProp) *scheduler.Task {
			if prop, err := json.Marshal(rp); err == nil {
				return &scheduler.Task{
					ClusterID:  h.clusterID,
					Type:       scheduler.BackupTask,
					Enabled:    true,
					Sched:      scheduler.Schedule{StartDate: now()},
					Properties: prop,
				}
			} else {
				panic(err)
			}
		}

		tasks := make([]*scheduler.Task, len(taskRetentions))
		for i, _ := range tasks {
			tasks[i] = newTask(taskRetentions[i])
		}

		Print("Given: backup task decorator")
		var s *backup.Service
		h.service.SetPropertiesDecorator(scheduler.BackupTask, s.TaskDecorator(h.service))

		Print("When: task is scheduled")

		for i, _ := range tasks {
			tasks[i] = newTask(taskRetentions[i])
			if err := h.service.PutTask(ctx, tasks[i]); err != nil {
				t.Fatal(err)
			}

			h.assertStatus(tasks[i], scheduler.StatusRunning)
			h.runner.Done()
			h.assertStatus(tasks[i], scheduler.StatusDone)
		}

		for i, _ := range taskRetentions {
			newRetMap := make(backup.RetentionMap)
			if i > 0 {
				for k, v := range taskRetentions[i-1].RetentionMap {
					newRetMap[k] = v
				}
			}
			newRetMap[tasks[i].ID] = backup.RetentionPolicy{
				Retention:     taskRetentions[i].Retention,
				RetentionDays: taskRetentions[i].RetentionDays,
			}
			taskRetentions[i].RetentionMap = newRetMap
		}

		Print("And: properties are decorated")
		for i, v := range h.runner.Properties() {
			var res retentionProp
			if err := json.Unmarshal(v, &res); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(res, taskRetentions[i]); diff != "" {
				t.Fatal(diff)
			}
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
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
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
		if err := h.service.Resume(ctx, h.clusterID, true, false, false); err != nil {
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
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 status is StatusStopped")
		h.assertStatus(task0, scheduler.StatusStopped)

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false, false, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task1 is not executed")
		h.assertNotStatus(task1, scheduler.StatusRunning)
	})

	t.Run("suspend and start allowed task and resume", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task0 (mock type) is scheduled now")
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 (mock type) runs")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: scheduler is suspended with allow 1_1_restore task")
		if err := h.service.Suspend(ctx, h.clusterID, scheduler.One2OneRestoreTask.String(), scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		s := h.service.SuspendStatus(ctx, h.clusterID)
		if !s.Suspended {
			t.Fatal("Expected suspended")
		}
		if s.AllowTask.TaskType != scheduler.One2OneRestoreTask {
			t.Fatalf("Expected allow task %s, got %s", scheduler.One2OneRestoreTask, s.AllowTask)
		}

		Print("Then: task0 (mock type) is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task0 (mock type) cannot be started")
		h.assertError(h.service.StartTask(ctx, task0), "suspended")

		Print("When: task1 (1_1_restore type) is scheduled in now")
		task1 := h.makeTaskOfTypeWithStartDate(scheduler.One2OneRestoreTask, now())
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}
		Print("Then: task1 (1_1_restore type) runs")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false, false, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 (mock type) is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task1 (1_1_restore type) is running")
		h.assertStatus(task1, scheduler.StatusRunning)
	})

	t.Run("suspend with running allowed task and resume", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task0 (mock type) is scheduled now")
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 runs")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: task1 (1_1_restore type) is scheduled in now")
		task1 := h.makeTaskOfTypeWithStartDate(scheduler.One2OneRestoreTask, now())
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}

		Print("Then: task1 runs")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: scheduler is suspended with allow 1_1_restore task")
		if err := h.service.Suspend(ctx, h.clusterID, scheduler.One2OneRestoreTask.String(), scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		s := h.service.SuspendStatus(ctx, h.clusterID)
		if !s.Suspended {
			t.Fatal("Expected suspended")
		}
		if s.AllowTask.TaskType != scheduler.One2OneRestoreTask {
			t.Fatalf("Expected allow task %s, got %s", scheduler.One2OneRestoreTask, s.AllowTask)
		}

		Print("Then: task0 (mock type) is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task0 cannot be started")
		h.assertError(h.service.StartTask(ctx, task0), "suspended")

		Print("Then: task1 (1_1_restore type) is running")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false, false, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 (mock type) is not executed")
		h.assertNotStatus(task0, scheduler.StatusRunning)

		Print("And: task1 (1_1_restore type) is running")
		h.assertStatus(task1, scheduler.StatusRunning)
	})

	t.Run("suspend and resume with start tasks missed activation", func(t *testing.T) {
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

		Print("When: task3 is scheduled in the near future")
		nearFutureDelta := 5 * time.Second
		task3 := h.makeTaskWithStartDate(now().Add(nearFutureDelta))
		if err := h.service.PutTask(ctx, task3); err != nil {
			t.Fatal(err)
		}

		Print("Then: task3 is not executed")
		h.assertNotStatus(task3, scheduler.StatusRunning)

		Print("When: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("When: near future is now")
		time.Sleep(nearFutureDelta)

		Print("And: scheduler is resumed with start tasks missed activation")
		if err := h.service.Resume(t.Context(), h.clusterID, true, true, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports not suspended status")
		if h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected not suspended")
		}

		Print("And: task0 is executed (because start tasks)")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("And: task1 is not executed (because it is scheduled for the future)")
		h.assertNotStatus(task1, scheduler.StatusRunning)

		Print("And: task3 is executed (because it missed its activation)")
		h.assertStatus(task3, scheduler.StatusRunning)
	})

	t.Run("suspend and resume with no continue", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task0 is scheduled now")
		task0 := h.makeTaskWithStartDate(now())
		task0.ID = uuid.Nil
		task0.Sched.NumRetries = 1
		task0.Sched.RetryWait = duration.Duration(time.Second)
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 runs")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: scheduler is suspended with no continue")
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		s := h.service.SuspendStatus(ctx, h.clusterID)
		if !s.Suspended {
			t.Fatal("Expected suspended")
		}

		Print("When: scheduler is resumed with no continue")
		if err := h.service.Resume(ctx, h.clusterID, true, false, true); err != nil {
			t.Fatal(err)
		}

		props := []json.RawMessage{
			json.RawMessage(`{}`),
			json.RawMessage(`{"continue":false}`),
			json.RawMessage(`{}`),
		}

		Print("Then: task0 is executed 2 times")
		h.assertStatus(task0, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task0, scheduler.StatusError)

		h.assertStatus(task0, scheduler.StatusRunning)
		h.runner.Error()
		h.assertStatus(task0, scheduler.StatusError)

		Print("And: properties are preserved")
		if diff := cmp.Diff(h.runner.Properties(), props); diff != "" {
			t.Fatal(diff)
		}
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

	t.Run("suspend task with allow task", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("When: task0 (mock type) is scheduled now")
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(ctx, task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 (mock type) is running")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: task1 (1_1_restore type) is scheduled now")
		task1 := h.makeTaskOfTypeWithStartDate(scheduler.One2OneRestoreTask, now())
		if err := h.service.PutTask(ctx, task1); err != nil {
			t.Fatal(err)
		}
		Print("Then: task1 (1_1_restore type) is running")
		h.assertStatus(task1, scheduler.StatusRunning)

		p, _ := json.Marshal(scheduler.SuspendProperties{
			Duration:   duration.Duration(time.Second),
			StartTasks: true,
			AllowTask:  scheduler.AllowedTaskType{scheduler.One2OneRestoreTask},
		})
		suspendTask := &scheduler.Task{
			ClusterID:  h.clusterID,
			Type:       scheduler.SuspendTask,
			Enabled:    true,
			Properties: p,
		}

		Print("When: suspend task with allowed 1_1_restore task type is scheduled")
		if err := h.service.PutTask(ctx, suspendTask); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 (mock type) status is stopped")
		h.assertStatus(task0, scheduler.StatusStopped)

		Print("And: task1 (1_1_restore type) is running")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("And: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: task0 (mock type) is automatically resumed")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("And: task1 (1_1_restore type) is continue to run")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("When: suspend task is finished")
		h.runner.Done()
		h.runner.Done()
		Print("Then: task0 (mock type) status is StatusDone")
		h.assertStatus(task0, scheduler.StatusDone)
		Print("And: task1 (1_1_restore type) status is StatusDone")
		h.assertStatus(task1, scheduler.StatusDone)
	})

	t.Run("suspend with dont stop running tasks", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()

		Print("When: task0 (mock type) is scheduled now")
		task0 := h.makeTaskWithStartDate(now())
		if err := h.service.PutTask(t.Context(), task0); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 runs")
		h.assertStatus(task0, scheduler.StatusRunning)

		Print("When: task1 (tablet_repair type) is scheduled now")
		task1 := h.makeTaskOfTypeWithStartDate(scheduler.TabletRepairTask, now())
		if err := h.service.PutTask(t.Context(), task1); err != nil {
			t.Fatal(err)
		}

		Print("Then: task1 runs")
		h.assertStatus(task1, scheduler.StatusRunning)

		Print("And: scheduler is not suspended with allow tablet_repair task and dont stop running tasks")
		err := h.service.Suspend(t.Context(), h.clusterID, scheduler.TabletRepairTask.String(), scheduler.SuspendPolicyFailIfRunningTasks, false)
		if err == nil || !errors.Is(err, scheduler.ErrNotAllowedTasksRunning) {
			t.Fatalf("Expected ErrNotAllowedTasksRunning, got %v", err)
		}

		Print("When: task0 is stopped")
		if err := h.service.StopTask(t.Context(), task0, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task0 is not executed")
		h.assertStatus(task0, scheduler.StatusStopped)

		Print("And: scheduler is suspended with allow tablet_repair task and dont stop running tasks")
		if err := h.service.Suspend(t.Context(), h.clusterID, scheduler.TabletRepairTask.String(), scheduler.SuspendPolicyFailIfRunningTasks, false); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("suspend with no continue performs cleanup", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()

		Print("When: snapshots of system_schema are taken")
		for i, host := range testconfig.ManagedClusterHosts() {
			// Make snapshots with different tags for more coverage
			tag := backupspec.SnapshotTagAt(timeutc.Now().Add(-time.Duration(i) * time.Second))
			if err := h.client.TakeSnapshot(t.Context(), host, tag, "system_schema"); err != nil {
				t.Fatal(err)
			}
		}

		anySnapshotOnDisk := func() bool {
			for _, host := range testconfig.ManagedClusterHosts() {
				tags, err := h.client.Snapshots(t.Context(), host)
				if err != nil {
					t.Fatal(err)
				}
				if len(tags) > 0 {
					return true
				}
			}
			return false
		}

		Print("Then: they are stored on nodes disks")
		if !anySnapshotOnDisk() {
			t.Fatal("Expected to find snapshot on disk after creating them manually")
		}

		Print("When: scheduler is suspended with no continue and allowed backup task")
		if err := h.service.Suspend(t.Context(), h.clusterID, scheduler.BackupTask.String(), scheduler.SuspendPolicyStopRunningTasks, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: snapshots are preserved")
		if !anySnapshotOnDisk() {
			t.Fatal("Expected to find snapshot on disk after suspend with allowed backup task")
		}

		Print("When: scheduler is resumed")
		if err := h.service.Resume(t.Context(), h.clusterID, false, false, true); err != nil {
			t.Fatal(err)
		}

		Print("And: scheduler is suspended with no continue and allowed tablet_repair task")
		if err := h.service.Suspend(t.Context(), h.clusterID, scheduler.TabletRepairTask.String(), scheduler.SuspendPolicyStopRunningTasks, true); err != nil {
			t.Fatal(err)
		}

		Print("Then: snapshots are deleted")
		if anySnapshotOnDisk() {
			t.Fatal("Expected snapshots to be cleaned up after suspend with allowed tablet_repair task")
		}
	})

	t.Run("put task when suspended", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		Print("Given: scheduler is suspended")
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
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
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
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
		if err := h.service.Resume(ctx, h.clusterID, true, false, false); err != nil {
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
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: task is not executed")
		h.assertNotStatus(task, scheduler.StatusRunning)

		Print("When: task start date passes by")
		time.Sleep(wait)

		Print("And: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false, false, false); err != nil {
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
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports suspended status")
		if !h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected suspended")
		}

		Print("And: suspending it again has no side effects")
		if err := h.service.Suspend(ctx, h.clusterID, "", scheduler.SuspendPolicyStopRunningTasks, false); err != nil {
			t.Fatal(err)
		}

		Print("When: scheduler is resumed")
		if err := h.service.Resume(ctx, h.clusterID, false, false, false); err != nil {
			t.Fatal(err)
		}

		Print("Then: scheduler reports not suspended status")
		if h.service.IsSuspended(ctx, h.clusterID) {
			t.Fatal("Expected resumed")
		}
	})

	t.Run("task out of maintenance window", func(t *testing.T) {
		h := newSchedTestHelper(t, session)
		defer h.close()
		ctx := context.Background()

		// Create ad-hoc window.
		// Watch out for the time near the midnight.
		stop := now().Add(15 * time.Second)
		start := stop.Add(15 * time.Second)
		if start.Hour() < stop.Hour() {
			time.Sleep(2 * time.Minute)
			stop = now().Add(15 * time.Second)
			start = stop.Add(15 * time.Second)
		}

		timeToDuration := func(t time.Time) time.Duration {
			return t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
		}

		window := []scheduler.WeekdayTime{
			{WeekdayTime: sched.WeekdayTime{Weekday: sched.EachDay, Time: 0}},
			{WeekdayTime: sched.WeekdayTime{Weekday: sched.EachDay, Time: timeToDuration(stop)}},
			{WeekdayTime: sched.WeekdayTime{Weekday: sched.EachDay, Time: timeToDuration(start)}},
			{WeekdayTime: sched.WeekdayTime{Weekday: sched.EachDay, Time: 24*time.Hour - time.Second}},
		}

		Print("When: task is scheduled")
		task := h.makeTask(scheduler.Schedule{
			StartDate: now(),
			Window:    window,
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("Then: task goes out of window")
		WaitCond(h.t, func() bool {
			v := h.getStatus(task)
			return v == scheduler.StatusWaiting
		}, _interval, 2*time.Minute)

		Print("Then: task starts in the next window")
		WaitCond(h.t, func() bool {
			v := h.getStatus(task)
			return v == scheduler.StatusRunning
		}, _interval, 2*time.Minute)

		h.runner.Done()
		Print("Then: task stops with the status done")
		h.assertStatus(task, scheduler.StatusDone)
	})

	t.Run("task ends with context error", func(t *testing.T) {
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
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task ends with context.Canceled")
		h.runner.in <- context.Canceled

		Print("Then: task ends with status error")
		h.assertStatus(task, scheduler.StatusError)

		Print("When: another task is scheduled")
		task = h.makeTask(scheduler.Schedule{
			StartDate: now(),
		})
		if err := h.service.PutTask(ctx, task); err != nil {
			t.Fatal(err)
		}

		Print("Then: task runs")
		h.assertStatus(task, scheduler.StatusRunning)

		Print("When: task ends with context.DeadlineExceeded")
		h.runner.in <- context.DeadlineExceeded

		Print("Then: task ends with status error")
		h.assertStatus(task, scheduler.StatusError)
	})
}
