// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"sort"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const (
	week = time.Duration(7) * 24 * time.Hour
)

func TestStatusMarshalText(t *testing.T) {
	statuses := []Status{
		StatusNew,
		StatusRunning,
		StatusStopped,
		StatusDone,
		StatusError,
		StatusAborted,
		StatusMissed,
	}

	var v Status
	for i, s := range statuses {
		b, err := s.MarshalText()
		if err != nil {
			t.Error(err)
		}
		if err := v.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if v != s {
			t.Error(i, "expected", s, "got", v)
		}
	}
}

func TestTaskValidate(t *testing.T) {
	table := []struct {
		Task  Task
		Valid bool
	}{
		{
			Task: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 1*time.Minute, 3),
			},
			Valid: false,
		},
		{
			Task: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*time.Minute, 3),
			},
			Valid: false,
		},
		{
			Task: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*2*retryTaskWait, 3),
			},
			Valid: false,
		},
		{
			Task: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*2*retryTaskWait+1, 3),
			},
			Valid: true,
		},
		{
			Task: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 10*24*time.Hour, 3),
			},
			Valid: true,
		},
	}

	for i, task := range table {
		err := task.Task.Validate()
		if task.Valid {
			if err != nil {
				t.Errorf("Expected valid task nr %d, error=%s, %v ", i, err, task.Task)
			}
		} else {
			if err == nil {
				t.Errorf("Expected invalid task nr %d, error=%s, %v ", i, err, task.Task)
			}
		}
	}
}

func makeSchedule(startDate time.Time, interval time.Duration, numRetries int) Schedule {
	return Schedule{
		StartDate:  startDate,
		Interval:   duration.Duration(interval),
		NumRetries: numRetries,
	}
}

func makeHistory(startDate time.Time, runStatus ...Status) []*Run {
	runs := make([]*Run, 0, len(runStatus))
	for i, s := range runStatus {
		runs = append(runs, &Run{
			ID:        uuid.NewTime(),
			StartTime: startDate.Add(time.Duration(i) * retryTaskWait),
			Status:    s,
		})
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].StartTime.After(runs[j].StartTime)
	})
	return runs
}

func TestSchedNextActivation(t *testing.T) {
	t.Parallel()

	var (
		now = timeutc.Now()
		t0  = now.AddDate(0, 0, -7)
		t1  = now.AddDate(0, 0, -2)
	)

	table := []struct {
		Name       string
		Schedule   Schedule
		Suspended  bool
		History    []*Run
		Activation time.Time
	}{
		{
			Name:       "no history old start with retries",
			Schedule:   makeSchedule(t0, week, 2),
			Activation: now,
		},
		{
			Name:       "no history start now",
			Schedule:   makeSchedule(now.Add(-time.Second), week, 2),
			Activation: now,
		},
		{
			Name:       "no history start in future",
			Schedule:   makeSchedule(now.Add(time.Second), week, 2),
			Activation: now.Add(time.Second),
		},
		{
			Name:       "short history 1 retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError),
			Activation: now,
		},
		{
			Name:       "short history 2 retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError, StatusError),
			Activation: now,
		},
		{
			Name:       "short (recent) history, retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(now.Add(-retryTaskWait/2), StatusError),
			Activation: now.Add(retryTaskWait / 2),
		},
		{
			Name:       "full history too many activations to retry, full interval",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError, StatusError, StatusError),
			Activation: t0.AddDate(0, 0, 7),
		},
		{
			Name:       "full history old activations retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    append(makeHistory(t1, StatusError), makeHistory(now.AddDate(0, 0, -5), StatusError, StatusError)...),
			Activation: now,
		},
		{
			Name:       "full history with DONE retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError, StatusDone, StatusError),
			Activation: now,
		},
		{
			Name:       "full history with STOPPED retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError, StatusStopped, StatusError),
			Activation: now,
		},
		{
			Name:       "one shot short history 1 retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError),
			Activation: now,
		},
		{
			Name:       "one shot short history 2 retry",
			Schedule:   makeSchedule(t0, week, 2),
			History:    makeHistory(t1, StatusError, StatusError),
			Activation: now,
		},
		{
			Name:       "one shot full history too many activations to retry no retry",
			Schedule:   makeSchedule(t0, 0, 2),
			History:    makeHistory(t1, StatusError, StatusError, StatusError),
			Activation: time.Time{},
		},
		{
			Name:       "no retry short history 1 full interval",
			Schedule:   makeSchedule(t0, week, 0),
			History:    makeHistory(t1, StatusError),
			Activation: t0.AddDate(0, 0, 7),
		},
		{
			Name:       "one shot aborted full history retry",
			Schedule:   makeSchedule(t0, 0, 2),
			History:    makeHistory(t1, StatusError, StatusError, StatusAborted),
			Activation: now,
		},
		{
			Name:       "no retry aborted short history 1 retry",
			Schedule:   makeSchedule(t0, week, 0),
			History:    makeHistory(t1, StatusAborted),
			Activation: now,
		},
		{
			Name:      "suspended one shot no history start now",
			Schedule:  makeSchedule(now.Add(-time.Second), 0, 2),
			Suspended: true,
		},
		{
			Name:       "suspended one shot no history start in future",
			Schedule:   makeSchedule(now.Add(time.Second), 0, 2),
			Suspended:  true,
			Activation: now.Add(time.Second),
		},
		{
			Name:       "suspended no history start now",
			Schedule:   makeSchedule(now.Add(-time.Second), week, 2),
			Suspended:  true,
			Activation: now.Add(-time.Second).Add(week),
		},
		{
			Name:       "suspended short history 1 retry",
			Schedule:   makeSchedule(t0, week, 2),
			Suspended:  true,
			History:    makeHistory(t1, StatusError),
			Activation: now,
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			if activation := test.Schedule.NextActivation(now, test.Suspended, test.History); activation != test.Activation {
				t.Errorf("NextActivation() = %s, expected %s", activation, test.Activation)
			}
		})
	}
}

func TestConsecutiveErrorCount(t *testing.T) {
	now := timeutc.Now()
	t0 := now.AddDate(0, 0, -1)
	t1 := now.Add(-15 * time.Minute)

	table := []struct {
		Name       string
		Schedule   Schedule
		History    []*Run
		ErrorCount int
	}{
		{
			"counting no running errors",
			makeSchedule(t0, 0, 3),
			makeHistory(t1, StatusDone, StatusAborted),
			0,
		},
		{
			"counting running errors",
			makeSchedule(t0, 0, 3),
			makeHistory(t1, StatusDone, StatusError, StatusError),
			2,
		},
		{
			"counting running errors after threshold",
			makeSchedule(t0, 10*time.Minute, 3),
			makeHistory(t1, StatusError, StatusError, StatusError),
			2,
		},
		{
			"counting no runs",
			makeSchedule(t0, 10*time.Minute, 3),
			nil,
			0,
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			if got := test.Schedule.ConsecutiveErrorCount(test.History, now); got != test.ErrorCount {
				t.Errorf("Got %d, expects %d", got, test.ErrorCount)
			}
		})
	}
}
