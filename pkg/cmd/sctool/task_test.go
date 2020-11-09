// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func TestTaskListSortingByStatus(t *testing.T) {
	t.Parallel()

	newTask := &managerclient.ExtendedTask{Status: "NEW"}
	runningTask := &managerclient.ExtendedTask{Status: "RUNNING"}
	stoppedTask := &managerclient.ExtendedTask{Status: "STOPPED"}
	doneTask := &managerclient.ExtendedTask{Status: "DONE"}
	errorTask := &managerclient.ExtendedTask{Status: "ERROR"}
	abortedTask := &managerclient.ExtendedTask{Status: "ABORTED"}

	input := managerclient.ExtendedTaskSlice{
		abortedTask,
		errorTask,
		doneTask,
		stoppedTask,
		runningTask,
		newTask,
	}
	expected := managerclient.ExtendedTaskSlice{
		newTask,
		runningTask,
		stoppedTask,
		doneTask,
		errorTask,
		abortedTask,
	}

	sortTasksByStatus(input)
	if diff := cmp.Diff(input, expected, testutils.DateTimeComparer()); diff != "" {
		t.Fatal(diff)
	}
}

func TestTaskTimeSortingFunctions(t *testing.T) {
	t.Parallel()

	now := timeutc.Now()
	t0 := &managerclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Millisecond)),
		StartTime:      strfmt.DateTime(now.Add(time.Millisecond)),
		EndTime:        strfmt.DateTime(now.Add(time.Millisecond)),
	}
	t1 := &managerclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Second)),
		StartTime:      strfmt.DateTime(now.Add(time.Second)),
		EndTime:        strfmt.DateTime(now.Add(time.Second)),
	}
	t2 := &managerclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Minute)),
		StartTime:      strfmt.DateTime(now.Add(time.Minute)),
		EndTime:        strfmt.DateTime(now.Add(time.Minute)),
	}
	notStartedTask := &managerclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now),
	}
	runningTask := &managerclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now),
		StartTime:      strfmt.DateTime(now),
	}

	table := []struct {
		Name         string
		SortFunction func(managerclient.ExtendedTaskSlice)
		Input        managerclient.ExtendedTaskSlice
		Expected     managerclient.ExtendedTaskSlice
	}{
		{
			Name:         "sort by by next activation time",
			SortFunction: sortTasksByNextActivation,
			Input:        managerclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     managerclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sort by by start time",
			SortFunction: sortTasksByStartTime,
			Input:        managerclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     managerclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sort by by end time",
			SortFunction: sortTasksByEndTime,
			Input:        managerclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     managerclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sorting by start time with not started task",
			SortFunction: sortTasksByStartTime,
			Input:        managerclient.ExtendedTaskSlice{t0, notStartedTask},
			Expected:     managerclient.ExtendedTaskSlice{notStartedTask, t0},
		},
		{
			Name:         "sorting by end time with running task",
			SortFunction: sortTasksByStartTime,
			Input:        managerclient.ExtendedTaskSlice{t0, runningTask},
			Expected:     managerclient.ExtendedTaskSlice{runningTask, t0},
		},
	}

	cmpOptions := cmp.Options{testutils.DateTimeComparer()}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			test.SortFunction(test.Input)
			if diff := cmp.Diff(test.Input, test.Expected, cmpOptions); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
