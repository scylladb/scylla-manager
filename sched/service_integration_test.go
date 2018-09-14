// Copyright (C) 2017 ScyllaDB

// +build all integration

//go:generate mockgen -destination mock_runner_test.go -mock_names Runner=MockRunner -package sched github.com/scylladb/mermaid/sched/runner Runner

package sched

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	"github.com/scylladb/gocqlx"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/uuid"
)

type descriptorMatcher struct {
	d runner.Descriptor
}

func (m descriptorMatcher) Matches(v interface{}) bool {
	if m.d.ClusterID == uuid.Nil {
		return true
	}
	return m.d == v.(runner.Descriptor)
}

func (m descriptorMatcher) String() string {
	return fmt.Sprint(m.d)
}

func putTask(t *testing.T, session *gocql.Session, ctx context.Context, task *Task) {
	t.Helper()
	if err := task.Validate(); err != nil {
		t.Fatal(err)
	}

	stmt, names := schema.SchedTask.Insert()
	q := gocqlx.Query(session.Query(stmt).WithContext(ctx), names).BindStruct(task)

	if err := q.ExecRelease(); err != nil {
		t.Fatal(err)
	}
}

func newScheduler(t *testing.T, session *gocql.Session) (*Service, *gomock.Controller) {
	t.Helper()
	ctrl := gomock.NewController(t)

	cp := func(ctx context.Context, id uuid.UUID) (*cluster.Cluster, error) {
		return &cluster.Cluster{
			ID: id,
		}, nil
	}

	s, err := NewService(session, cp, log.NewDevelopment().Named("sched"))
	if err != nil {
		t.Fatal(err)
	}
	s.SetRunner(mockTask, NewMockRunner(ctrl))

	return s, ctrl
}

func TestSchedInitOneShotIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow := func() time.Time {
		return baseTime
	}
	oldRetryTaskWait := retryTaskWait
	oldMonitorTaskInterval := monitorTaskInterval
	retryTaskWait = 5 * time.Second
	monitorTaskInterval = time.Millisecond

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }

	defer func() {
		retryTaskWait = oldRetryTaskWait
		monitorTaskInterval = oldMonitorTaskInterval
		timeNow = time.Now
		reschedTaskDone = func(*Task) {}
	}()

	taskStart := timeutc.Now().Add(taskStartNowSlack + time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{StartDate: taskStart, NumRetries: 2},
	}
	putTask(t, session, ctx, task)

	m := descriptorMatcher{}

	expect := s.runners[mockTask].(*MockRunner).EXPECT()
	gomock.InOrder(
		expect.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(func(_, d, _ interface{}) {
			tick()
			m.d = d.(runner.Descriptor)
		}),
		expect.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil).Times(4).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.Status(gomock.Any(), m).Return(runner.StatusStopping, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.Status(gomock.Any(), m).Return(runner.StatusStopped, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.Init(ctx)
	<-ch
	s.Close()
	runs, err := s.GetLastRun(ctx, task, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("len(runs) (%d) != 1", len(runs))
	}
	if runs[0].ID != m.d.RunID {
		t.Fatal("id mismatch, expected:", m.d.RunID, "but got", runs[0].ID)
	}
	if runs[0].Status != runner.StatusStopped {
		t.Fatal("wrong status", runs[0].ID, runs[0].Status)
	}
}

func TestSchedInitOneShotRunningIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	defer s.Close()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow := func() time.Time {
		return baseTime
	}
	oldRetryTaskWait := retryTaskWait
	oldMonitorTaskInterval := monitorTaskInterval
	retryTaskWait = 5 * time.Second
	monitorTaskInterval = time.Millisecond
	defer func() {
		retryTaskWait = oldRetryTaskWait
		monitorTaskInterval = oldMonitorTaskInterval
		timeNow = time.Now
		reschedTaskDone = func(*Task) {}
	}()

	taskStart := timeutc.Now().Add(time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{StartDate: taskStart, NumRetries: 2},
	}
	putTask(t, session, ctx, task)

	storedRun := &Run{
		ID:        uuid.NewTime(),
		Type:      task.Type,
		ClusterID: clusterID,
		TaskID:    task.ID,
		Status:    runner.StatusRunning,
		StartTime: taskStart,
	}
	if err := s.putRun(ctx, storedRun); err != nil {
		t.Fatal("failed to put run", storedRun, err)
	}

	m := descriptorMatcher{
		d: runner.Descriptor{
			ClusterID: storedRun.ClusterID,
			TaskID:    storedRun.TaskID,
			RunID:     storedRun.ID,
		},
	}

	expect := s.runners[mockTask].(*MockRunner).EXPECT()
	gomock.InOrder(
		expect.Status(gomock.Any(), m).Return(runner.StatusStopped, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.Init(ctx)
	runs, err := s.GetLastRun(ctx, task, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("len(runs) (%d) != 1", len(runs))
	}
	if runs[0].ID != storedRun.ID {
		t.Fatal("id mismatch, expected:", storedRun.ID, "but got", runs[0].ID)
	}
	if runs[0].Status != runner.StatusStopped {
		t.Fatal("wrong status", runs[0].ID, runs[0].Status)
	}
}

func TestSchedInitOneShotRetryIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow := func() time.Time {
		return baseTime
	}
	oldRetryTaskWait := retryTaskWait
	oldMonitorTaskInterval := monitorTaskInterval
	retryTaskWait = 5 * time.Second
	monitorTaskInterval = time.Millisecond

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }

	defer func() {
		retryTaskWait = oldRetryTaskWait
		monitorTaskInterval = oldMonitorTaskInterval
		timeNow = time.Now
		reschedTaskDone = func(*Task) {}
	}()

	taskStart := timeutc.Now().Add(time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{StartDate: taskStart, NumRetries: 2},
	}
	putTask(t, session, ctx, task)

	storedRun := &Run{
		ID:        uuid.NewTime(),
		Type:      task.Type,
		ClusterID: clusterID,
		TaskID:    task.ID,
		Status:    runner.StatusRunning,
		StartTime: taskStart,
	}
	if err := s.putRun(ctx, storedRun); err != nil {
		t.Fatal("failed to put run", storedRun, err)
	}

	m := descriptorMatcher{}

	expect := s.runners[mockTask].(*MockRunner).EXPECT()
	gomock.InOrder(
		expect.Status(gomock.Any(), m).Return(runner.StatusError, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.Run(gomock.Any(), m, gomock.Any()).Return(nil).Do(func(_, d, _ interface{}) {
			tick()
			m.d = d.(runner.Descriptor)
		}),

		expect.Status(gomock.Any(), m).Return(runner.StatusRunning, "", nil).Times(4).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.Status(gomock.Any(), m).Return(runner.StatusStopping, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.Status(gomock.Any(), m).Return(runner.StatusStopped, "", nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.Init(ctx)
	<-ch
	s.Close()
	runs, err := s.GetLastRun(ctx, task, 10)
	if err != nil {
		t.Log(err)
		t.Fatal()
	}
	if len(runs) != 2 {
		t.Fatalf("len(runs) (%d) != 2", len(runs))
	}

	for i, r := range []struct {
		ID     uuid.UUID
		Status runner.Status
	}{
		{m.d.RunID, runner.StatusStopped},
		{storedRun.ID, runner.StatusError},
	} {
		if runs[i].ID != r.ID {
			t.Fatal("id mismatch, expected:", runs[i].ID, "but got", r.ID)
		}
		if runs[i].Status != r.Status {
			t.Fatal("wrong status", r.ID, "expected", runs[i].Status, "got", r.Status)
		}
	}
}

func TestSchedInitRepeatingIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow := func() time.Time {
		return baseTime
	}
	oldRetryTaskWait := retryTaskWait
	oldMonitorTaskInterval := monitorTaskInterval
	retryTaskWait = 5 * time.Second
	monitorTaskInterval = time.Millisecond

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }

	defer func() {
		retryTaskWait = oldRetryTaskWait
		monitorTaskInterval = oldMonitorTaskInterval
		timeNow = time.Now
		reschedTaskDone = func(*Task) {}
	}()

	taskStart := timeutc.Now().Add(time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{Interval: Duration(2 * 24 * time.Hour), NumRetries: 3, StartDate: taskStart},
	}
	putTask(t, session, ctx, task)

	m := make([]descriptorMatcher, task.Sched.NumRetries, task.Sched.NumRetries)

	runNum := 0
	expect := s.runners[mockTask].(*MockRunner).EXPECT()
	calls := make([]*gomock.Call, 0, task.Sched.NumRetries)
	for i := 0; i < task.Sched.NumRetries; i++ {
		i := i
		calls = append(calls,
			expect.Run(gomock.Any(), m[i], gomock.Any()).Return(nil).Do(func(_, d, _ interface{}) {
				tick()
				m[i].d = d.(runner.Descriptor)
				runNum++
			}),

			expect.Status(gomock.Any(), m[i]).Return(runner.StatusRunning, "", nil).Times(4).Do(func(_ ...interface{}) {
				tick()
			}),

			expect.Status(gomock.Any(), m[i]).Return(runner.StatusError, "", nil).Do(func(_ ...interface{}) {
				tick()
			}),
		)
	}
	gomock.InOrder(calls...)

	s.Init(ctx)
	for i := 0; i < task.Sched.NumRetries; i++ {
		<-ch
	}
	s.Close()
	runs, err := s.GetLastRun(ctx, task, 10)
	if err != nil {
		t.Log(err)
		t.Fatal()
	}
	if len(runs) != runNum {
		t.Fatalf("len(runs) (%d) != runNum (%d)", len(runs), runNum)
	}

	for i, id := range []uuid.UUID{m[2].d.RunID, m[1].d.RunID, m[0].d.RunID} {
		if runs[i].ID != id {
			t.Fatal("id mismatch, expected:", runs[i].ID, "but got", id)
		}
		if runs[i].Status != runner.StatusError {
			t.Fatal("wrong status", id, runs[i].Status)
		}
	}
}
