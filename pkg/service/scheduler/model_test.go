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
		T Task
		V bool
	}{
		{
			T: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 1*time.Minute, 3),
			},
			V: false,
		},
		{
			T: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*time.Minute, 3),
			},
			V: false,
		},
		{
			T: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*2*retryTaskWait, 3),
			},
			V: false,
		},
		{
			T: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 3*2*retryTaskWait+1, 3),
			},
			V: true,
		},
		{
			T: Task{
				ClusterID: uuid.MustRandom(),
				ID:        uuid.MustRandom(),
				Type:      RepairTask,
				Sched:     makeSchedule(timeutc.Now(), 10*24*time.Hour, 3),
			},
			V: true,
		},
	}

	for i, task := range table {
		err := task.T.Validate()
		if task.V {
			if err != nil {
				t.Errorf("Expected valid task nr %d, error=%s, %v ", i, err, task.T)
			}
		} else {
			if err == nil {
				t.Errorf("Expected invalid task nr %d, error=%s, %v ", i, err, task.T)
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

	now := timeutc.Now()
	t0 := now.AddDate(0, 0, -7)
	t1 := now.AddDate(0, 0, -2)

	table := []struct {
		S Schedule
		H []*Run
		A time.Time
	}{
		// no history, old start with retries
		{
			S: makeSchedule(t0, week, 2),
			A: now.Add(startTaskNowSlack),
		},
		// no history, start in future > startTaskNowSlack
		{
			S: makeSchedule(now.Add(startTaskNowSlack+time.Second), week, 2),
			A: now.Add(startTaskNowSlack + time.Second),
		},
		// no history, start in future < tastStartNowSlack
		{
			S: makeSchedule(now.Add(time.Second), week, 2),
			A: now.Add(retryTaskWait + time.Second),
		},
		// short history 1, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// short history 2, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// short (recent) history, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(now.Add(-retryTaskWait/2), StatusError),
			A: now.Add(retryTaskWait / 2),
		},
		// full history, too many activations to retry, full interval
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError, StatusError, StatusError),
			A: t0.AddDate(0, 0, 7),
		},
		// full history, old activations, retry
		{
			S: makeSchedule(t0, week, 2),
			H: append(makeHistory(t1, StatusError), makeHistory(now.AddDate(0, 0, -5), StatusError, StatusError)...),
			A: now.Add(startTaskNowSlack),
		},
		// full history with DONE, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError, StatusDone, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// full history with STOPPED, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError, StatusStopped, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// one shot, short history 1, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// one shot, short history 2, retry
		{
			S: makeSchedule(t0, week, 2),
			H: makeHistory(t1, StatusError, StatusError),
			A: now.Add(startTaskNowSlack),
		},
		// one shot, full history, too many activations to retry, no retry
		{
			S: makeSchedule(t0, 0, 2),
			H: makeHistory(t1, StatusError, StatusError, StatusError),
			A: time.Time{},
		},
		// no retry, short history 1, full interval
		{
			S: makeSchedule(t0, week, 0),
			H: makeHistory(t1, StatusError),
			A: t0.AddDate(0, 0, 7),
		},
		// one shot aborted, full history, retry
		{
			S: makeSchedule(t0, 0, 2),
			H: makeHistory(t1, StatusError, StatusError, StatusAborted),
			A: now.Add(startTaskNowSlack),
		},
		// no retry aborted, short history 1, retry
		{
			S: makeSchedule(t0, week, 0),
			H: makeHistory(t1, StatusAborted),
			A: now.Add(startTaskNowSlack),
		},
	}

	for i, test := range table {
		if activation := test.S.NextActivation(now, test.H); activation != test.A {
			t.Error(i, "expected", test.A, "got", activation)
		}
	}
}

func TestConsecutiveErrorCount(t *testing.T) {
	now := timeutc.Now()
	t0 := now.AddDate(0, 0, -1)
	t1 := now.Add(-15 * time.Minute)

	table := []struct {
		N string
		S Schedule
		R []*Run
		E int
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
		t.Run(test.N, func(t *testing.T) {
			if got := test.S.ConsecutiveErrorCount(test.R, now); got != test.E {
				t.Errorf("Got %d, expects %d", got, test.E)
			}
		})
	}
}
