// Copyright (C) 2017 ScyllaDB

package sched

import (
	"sort"
	"testing"
	"time"

	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

func makeSchedule(startDate time.Time, interval, numRetries int) Schedule {
	return Schedule{
		StartDate:  startDate,
		Interval:   Duration(time.Duration(interval) * 24 * time.Hour),
		NumRetries: numRetries,
	}
}

func makeHistory(startDate time.Time, runStatus ...runner.Status) []*Run {
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
	t1 := t0.AddDate(0, 0, 2)

	table := []struct {
		schedule       Schedule
		history        []*Run
		nextActivation time.Time
	}{
		// no history, old start with retries
		{
			schedule:       makeSchedule(t0, 7, 3),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// no history, start in future > taskStartNowSlack
		{
			schedule:       makeSchedule(now.Add(taskStartNowSlack+time.Second), 7, 3),
			nextActivation: now.Add(taskStartNowSlack + time.Second),
		},
		// no history, start in future < tastStartNowSlack
		{
			schedule:       makeSchedule(now.Add(time.Second), 7, 3),
			nextActivation: now.Add(retryTaskWait + time.Second),
		},
		// short history 1, retry
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// short history 2, retry
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// short (recent) history, retry
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(now.Add(-retryTaskWait/2), runner.StatusError),
			nextActivation: now.Add(retryTaskWait / 2),
		},
		// full history, too many activations to retry, full interval
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError, runner.StatusError),
			nextActivation: t0.AddDate(0, 0, 7),
		},
		// full history with DONE, retry
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusDone, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// full history with STOPPED, retry
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusStopped, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// one shot, short history 1, retry
		{
			schedule:       makeSchedule(t0, 0, 3),
			history:        makeHistory(t1, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// one shot, short history 2, retry
		{
			schedule:       makeSchedule(t0, 0, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// one shot, full history, too many activations to retry, no retry
		{
			schedule:       makeSchedule(t0, 0, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError, runner.StatusError),
			nextActivation: time.Time{},
		},
		// no retry, short history 1, full interval
		{
			schedule:       makeSchedule(t0, 7, 0),
			history:        makeHistory(t1, runner.StatusError),
			nextActivation: t0.AddDate(0, 0, 7),
		},
		// one shot aborted, full history, retry
		{
			schedule:       makeSchedule(t0, 0, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError, runner.StatusAborted),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// no retry aborted, short history 1, retry
		{
			schedule:       makeSchedule(t0, 7, 0),
			history:        makeHistory(t1, runner.StatusAborted),
			nextActivation: now.Add(taskStartNowSlack),
		},
	}

	for i, test := range table {
		if activation := test.schedule.NextActivation(now, test.history); activation != test.nextActivation {
			t.Error(i, "expected", test.nextActivation, "got", activation)
		}
	}
}
