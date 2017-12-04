// Copyright (C) 2017 ScyllaDB

// +build all integration

package sched

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidmock"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/uuid"
)

type uuidMatcher struct {
	uuid *uuid.UUID
}

func (m uuidMatcher) Matches(x interface{}) bool {
	return *m.uuid == x.(uuid.UUID)
}

func (m uuidMatcher) String() string {
	return fmt.Sprintf("uuidMatcher against: %v", m.uuid)
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

	s, err := NewService(session, log.NewDevelopment().Named("sched"))
	if err != nil {
		t.Fatal(err)
	}
	s.SetRunner(mockTask, mermaidmock.NewMockRunner(ctrl))

	return s, ctrl
}

func TestSchedLoadTasksOneShotIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow = func() time.Time {
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

	taskStart := timeNow().UTC().Add(taskStartNowSlack + time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{StartDate: taskStart},
	}
	putTask(t, session, ctx, task)

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }
	newRunID := uuid.Nil
	expect := s.runners[mockTask].(*mermaidmock.MockRunner).EXPECT()
	gomock.InOrder(
		expect.RunTask(gomock.Any(), clusterID, gomock.Any(), gomock.Any()).Return(nil).Do(func(_, _, runID interface{}, _ ...interface{}) {
			tick()
			newRunID = runID.(uuid.UUID)
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusRunning, nil).Times(4).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusStopping, nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusStopped, nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.LoadTasks(ctx)
	<-ch
	s.Close(ctx)
	runs, err := s.GetLastRunN(ctx, task, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("len(runs) (%d) != 1", len(runs))
	}
	if runs[0].ID != newRunID {
		t.Fatal("id mismatch, expected:", newRunID, "but got", runs[0].ID)
	}
	if runs[0].Status != runner.StatusStopped {
		t.Fatal("wrong status", runs[0].ID, runs[0].Status)
	}
}

func TestSchedLoadTasksOneShotRunningIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	defer s.Close(ctx)
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow = func() time.Time {
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

	taskStart := timeNow().UTC().Add(time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{StartDate: taskStart},
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

	expect := s.runners[mockTask].(*mermaidmock.MockRunner).EXPECT()
	gomock.InOrder(
		expect.TaskStatus(gomock.Any(), clusterID, storedRun.ID, gomock.Any()).Return(runner.StatusStopped, nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.LoadTasks(ctx)
	runs, err := s.GetLastRunN(ctx, task, -1)
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

func TestSchedLoadTasksOneShotRetryIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow = func() time.Time {
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

	taskStart := timeNow().UTC().Add(time.Second)
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

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }
	newRunID := uuid.Nil
	expect := s.runners[mockTask].(*mermaidmock.MockRunner).EXPECT()
	gomock.InOrder(
		expect.TaskStatus(gomock.Any(), clusterID, storedRun.ID, gomock.Any()).Return(runner.StatusError, nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.RunTask(gomock.Any(), clusterID, gomock.Any(), gomock.Any()).Return(nil).Do(func(_, _, runID interface{}, _ ...interface{}) {
			tick()
			newRunID = runID.(uuid.UUID)
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusRunning, nil).Times(4).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusStopping, nil).Do(func(_ ...interface{}) {
			tick()
		}),

		expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID}, gomock.Any()).Return(runner.StatusStopped, nil).Do(func(_ ...interface{}) {
			tick()
		}),
	)

	s.LoadTasks(ctx)
	<-ch
	s.Close(ctx)
	runs, err := s.GetLastRunN(ctx, task, -1)
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
		{newRunID, runner.StatusStopped},
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

func TestSchedLoadTasksRepeatingIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	s, ctrl := newScheduler(t, session)
	defer ctrl.Finish()

	ctx := context.Background()
	baseTime := time.Date(2017, 11, 27, 14, 20, 0, 0, time.Local)
	tick := func() { baseTime = baseTime.Add(time.Second) }
	timeNow = func() time.Time {
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

	taskStart := timeNow().UTC().Add(time.Second)
	clusterID := uuid.MustRandom()

	task := &Task{ClusterID: clusterID, Type: mockTask, ID: uuid.MustRandom(), Name: "task1", Enabled: true,
		Sched: Schedule{IntervalDays: 2, NumRetries: 3, StartDate: taskStart},
	}
	putTask(t, session, ctx, task)

	ch := make(chan bool)
	reschedTaskDone = func(*Task) { ch <- true }
	newRunID := []uuid.UUID{uuid.Nil, uuid.Nil, uuid.Nil}
	runNum := 0
	expect := s.runners[mockTask].(*mermaidmock.MockRunner).EXPECT()

	calls := make([]*gomock.Call, 0, task.Sched.NumRetries)
	for i := 0; i < task.Sched.NumRetries; i++ {
		calls = append(calls,
			expect.RunTask(gomock.Any(), clusterID, gomock.Any(), gomock.Any()).Return(nil).Do(func(_, _, runID interface{}, _ ...interface{}) {
				tick()
				newRunID[runNum] = runID.(uuid.UUID)
				runNum++
			}),

			expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID[i]}, gomock.Any()).Return(runner.StatusRunning, nil).Times(4).Do(func(_ ...interface{}) {
				tick()
			}),

			expect.TaskStatus(gomock.Any(), clusterID, uuidMatcher{&newRunID[i]}, gomock.Any()).Return(runner.StatusError, nil).Do(func(_ ...interface{}) {
				tick()
			}),
		)
	}
	gomock.InOrder(calls...)

	s.LoadTasks(ctx)
	for i := 0; i < task.Sched.NumRetries; i++ {
		<-ch
	}
	s.Close(ctx)
	runs, err := s.GetLastRunN(ctx, task, -1)
	if err != nil {
		t.Log(err)
		t.Fatal()
	}
	if len(runs) != runNum {
		t.Fatalf("len(runs) (%d) != runNum (%d)", len(runs), runNum)
	}

	for i, id := range []uuid.UUID{newRunID[2], newRunID[1], newRunID[0]} {
		if runs[i].ID != id {
			t.Fatal("id mismatch, expected:", runs[i].ID, "but got", id)
		}
		if runs[i].Status != runner.StatusError {
			t.Fatal("wrong status", id, runs[i].Status)
		}
	}
}
