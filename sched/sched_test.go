// Copyright (C) 2017 ScyllaDB

package sched

import (
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/mermaid/uuid"
)

func makeSchedule(startDate time.Time, interval, numRetries int) Schedule {
	return Schedule{
		Repeat:       true,
		StartDate:    startDate,
		IntervalDays: interval,
		NumRetries:   numRetries,
	}
}

func makeHistory(startDate time.Time, runStatus ...Status) []*Run {
	runs := make([]*Run, 0, len(runStatus))
	for i, s := range runStatus {
		runs = append(runs, &Run{
			ID:        uuid.NewTime(),
			StartTime: startDate.Add(time.Duration(i) * time.Hour),
			Status:    s,
		})
	}
	return runs
}

func TestSchedNextActivation(t *testing.T) {
	now := time.Now().UTC()
	t0 := now.AddDate(0, 0, -7)
	t1 := t0.AddDate(0, 0, 2)

	table := []struct {
		schedule       Schedule
		history        []*Run
		nextActivation time.Time
	}{
		// no history
		{
			schedule:       makeSchedule(t0, 7, 3),
			nextActivation: now.Add(time.Hour),
		},
		// short (old) history 1
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, StatusError),
			nextActivation: now.Add(time.Hour),
		},
		// short (old) history 2
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, StatusError, StatusError),
			nextActivation: now.Add(time.Hour),
		},
		// short (recent) history
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(now.Add(-30*time.Minute), StatusError),
			nextActivation: now.Add(30 * time.Minute),
		},
		// full history, too many activations to retry again, waiting for full interval period.
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, StatusError, StatusError, StatusError),
			nextActivation: t1.Add(2*time.Hour).AddDate(0, 0, 7),
		},
		// full (old) history, retries allowed.
		{
			schedule:       makeSchedule(t0, 7, 3),
			history:        makeHistory(t1, StatusError, StatusStopped, StatusError),
			nextActivation: now.Add(time.Hour),
		},
	}

	for i, tc := range table {
		tc := tc
		t.Run(fmt.Sprintf("TestCase-%d", i), func(t *testing.T) {
			if activation := tc.schedule.nextActivation(now, tc.history); activation != tc.nextActivation {
				t.Logf("expected activation: %v, computed: %v", tc.nextActivation, activation)
				t.Fail()
			}
		})
	}
}
