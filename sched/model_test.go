// Copyright (C) 2017 ScyllaDB

package sched

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

func makeSchedule(startDate time.Time, interval, numRetries int) Schedule {
	return Schedule{
		StartDate:    startDate,
		IntervalDays: interval,
		NumRetries:   numRetries,
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
		// no history, start in future > tastStartNowSlack
		{
			schedule:       makeSchedule(now.Add(taskStartNowSlack+time.Second), 7, 3),
			nextActivation: now.Add(taskStartNowSlack + time.Second),
		},
		// no history, start in future < tastStartNowSlack
		{
			schedule:       makeSchedule(now.Add(time.Second), 7, 3),
			nextActivation: now.Add(retryTaskWait + time.Second),
		},
		// short (old) history 1
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// short (old) history 2
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
		// short (recent) history
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(now.Add(-retryTaskWait/2), runner.StatusError),
			nextActivation: now.Add(retryTaskWait / 2),
		},
		// full history, too many activations to retry again, waiting for full interval period.
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusError, runner.StatusError),
			nextActivation: t1.Add(2*retryTaskWait).AddDate(0, 0, 7),
		},
		// full (old) history, retries allowed.
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, runner.StatusError, runner.StatusStopped, runner.StatusError),
			nextActivation: now.Add(taskStartNowSlack),
		},
	}

	for i, tc := range table {
		tc := tc
		t.Run(fmt.Sprintf("TestCase-%d", i), func(t *testing.T) {
			if activation := tc.schedule.NextActivation(now, tc.history); activation != tc.nextActivation {
				t.Logf("expected activation: %v, computed: %v", tc.nextActivation, activation)
				t.Fail()
			}
		})
	}
}
