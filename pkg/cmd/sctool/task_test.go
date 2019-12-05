// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

func TestTaskListSortingByStatus(t *testing.T) {
	t.Parallel()

	newTask := &mermaidclient.ExtendedTask{Status: "NEW"}
	runningTask := &mermaidclient.ExtendedTask{Status: "RUNNING"}
	stoppedTask := &mermaidclient.ExtendedTask{Status: "STOPPED"}
	doneTask := &mermaidclient.ExtendedTask{Status: "DONE"}
	errorTask := &mermaidclient.ExtendedTask{Status: "ERROR"}
	abortedTask := &mermaidclient.ExtendedTask{Status: "ABORTED"}

	input := mermaidclient.ExtendedTaskSlice{
		abortedTask,
		errorTask,
		doneTask,
		stoppedTask,
		runningTask,
		newTask,
	}
	expected := mermaidclient.ExtendedTaskSlice{
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
	t0 := &mermaidclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Millisecond)),
		StartTime:      strfmt.DateTime(now.Add(time.Millisecond)),
		EndTime:        strfmt.DateTime(now.Add(time.Millisecond)),
	}
	t1 := &mermaidclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Second)),
		StartTime:      strfmt.DateTime(now.Add(time.Second)),
		EndTime:        strfmt.DateTime(now.Add(time.Second)),
	}
	t2 := &mermaidclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now.Add(time.Minute)),
		StartTime:      strfmt.DateTime(now.Add(time.Minute)),
		EndTime:        strfmt.DateTime(now.Add(time.Minute)),
	}
	notStartedTask := &mermaidclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now),
	}
	runningTask := &mermaidclient.ExtendedTask{
		NextActivation: strfmt.DateTime(now),
		StartTime:      strfmt.DateTime(now),
	}

	table := []struct {
		Name         string
		SortFunction func(mermaidclient.ExtendedTaskSlice)
		Input        mermaidclient.ExtendedTaskSlice
		Expected     mermaidclient.ExtendedTaskSlice
	}{
		{
			Name:         "sort by by next activation time",
			SortFunction: sortTasksByNextActivation,
			Input:        mermaidclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     mermaidclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sort by by start time",
			SortFunction: sortTasksByStartTime,
			Input:        mermaidclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     mermaidclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sort by by end time",
			SortFunction: sortTasksByEndTime,
			Input:        mermaidclient.ExtendedTaskSlice{t2, t1, t0},
			Expected:     mermaidclient.ExtendedTaskSlice{t0, t1, t2},
		},
		{
			Name:         "sorting by start time with not started task",
			SortFunction: sortTasksByStartTime,
			Input:        mermaidclient.ExtendedTaskSlice{t0, notStartedTask},
			Expected:     mermaidclient.ExtendedTaskSlice{notStartedTask, t0},
		},
		{
			Name:         "sorting by end time with running task",
			SortFunction: sortTasksByStartTime,
			Input:        mermaidclient.ExtendedTaskSlice{t0, runningTask},
			Expected:     mermaidclient.ExtendedTaskSlice{runningTask, t0},
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
