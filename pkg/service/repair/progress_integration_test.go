// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestProgressManagerIntegration(t *testing.T) {
	run := &Run{
		ClusterID: uuid.NewTime(),
		TaskID:    uuid.NewTime(),
		ID:        uuid.NewTime(),
		StartTime: timeutc.Now(),
	}

	opts := cmp.Options{
		cmp.AllowUnexported(JobExecution{}),
		UUIDComparer(),
		NearTimeComparer(5 * time.Millisecond),
		intervalComparer(),
	}

	t.Run("progress update sequence (Init,OnJobStart,OnScyllaJobStart,OnScyllaJobEnd,OnJobResult)", func(t *testing.T) {
		var (
			session = CreateSession(t)
		)

		ctx := context.Background()
		pm := newProgressManager(run, session, log.NewDevelopment())
		Print("When: run progress is initialized with incomplete values")
		if err := pm.Init(ctx, []*tableTokenRange{
			{
				Keyspace:   "k1",
				Table:      "t1",
				StartToken: 0,
				EndToken:   10,
				Replicas:   []string{"h1", "h2"},
			},
			{
				Keyspace:   "k1",
				Table:      "t1",
				StartToken: 11,
				EndToken:   20,
				Replicas:   []string{"h1", "h2"},
			},
		}); err != nil {
			t.Fatal(err)
		}

		Print("Then: progress rows are initialized with zero values")
		goldenProgress := []RunProgress{
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h1",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 2,
				Success:     0,
				Error:       0,
			},
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h2",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 2,
				Success:     0,
				Error:       0,
			},
		}
		updatedProgress := getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnJobStart called on progress manager")
		jobID := JobIDTuple{
			Master: "h1",
			ID:     1,
		}
		j := job{
			Host: "h1",
			Ranges: []*tableTokenRange{
				{
					Keyspace:   "k1",
					Table:      "t1",
					Pos:        1,
					StartToken: 0,
					EndToken:   10,
					Replicas:   []string{"h1", "h2"},
				},
			},
		}
		start := timeutc.Now()
		if err := pm.OnScyllaJobStart(ctx, j, jobID.ID); err != nil {
			t.Fatal(err)
		}
		Print("Then: job execution is updated with interval start")
		goldenJobs := []JobExecution{
			{
				interval:  interval{start, nil},
				ClusterID: run.ClusterID,
				TaskID:    run.TaskID,
				RunID:     run.ID,
				Keyspace:  "k1",
				Table:     "t1",
				Host:      "h1",
				JobID:     jobID,
			}, {
				interval:  interval{start, nil},
				ClusterID: run.ClusterID,
				TaskID:    run.TaskID,
				RunID:     run.ID,
				Keyspace:  "k1",
				Table:     "t1",
				Host:      "h2",
				JobID:     jobID,
			},
		}

		updatedJobs := getJobExecutions(run, session)
		if diff := cmp.Diff(goldenJobs, updatedJobs, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnJobEnd is called on progress manager")
		end := timeutc.Now()
		if err := pm.OnScyllaJobEnd(ctx, j, jobID.ID); err != nil {
			t.Fatal(err)
		}

		Print("Then: job execution is updated with interval end")
		goldenJobs = []JobExecution{
			{
				interval:  interval{start, &end},
				ClusterID: run.ClusterID,
				TaskID:    run.TaskID,
				RunID:     run.ID,
				Keyspace:  "k1",
				Table:     "t1",
				Host:      "h1",
				JobID:     jobID,
			}, {
				interval:  interval{start, &end},
				ClusterID: run.ClusterID,
				TaskID:    run.TaskID,
				RunID:     run.ID,
				Keyspace:  "k1",
				Table:     "t1",
				Host:      "h2",
				JobID:     jobID,
			},
		}

		updatedJobs = getJobExecutions(run, session)
		if diff := cmp.Diff(goldenJobs, updatedJobs, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnJobResult is called on progress manager")
		if err := pm.OnJobResult(ctx, jobResult{
			job: j,
			Err: nil,
		}); err != nil {
			t.Fatal(err)
		}

		Print("Then: progress is updated with success")
		goldenProgress = []RunProgress{
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h1",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 2,
				Success:     1,
				Error:       0,
			},
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h2",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 2,
				Success:     1,
				Error:       0,
			},
		}
		updatedProgress = getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("And: state is saved for completed range")
		goldenState := []*RunState{
			{
				ClusterID:  run.ClusterID,
				TaskID:     run.TaskID,
				RunID:      run.ID,
				Keyspace:   "k1",
				Table:      "t1",
				SuccessPos: []int{1},
				ErrorPos:   nil,
			},
		}
		states, err := pm.getState(run)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(goldenState, states, UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("restoring state", func(t *testing.T) {
		var (
			session = CreateSession(t)
		)

		table.RepairRunState.Insert()

		ctx := context.Background()
		run := *run
		run.PrevID = uuid.NewTime()
		pm := newProgressManager(&run, session, log.NewDevelopment())

		Print("When: there is present state of success at position 1")
		if err := table.RepairRunState.InsertQuery(session).BindStruct(&RunState{
			ClusterID:  run.ClusterID,
			TaskID:     run.TaskID,
			RunID:      run.PrevID,
			Keyspace:   "k1",
			Table:      "t1",
			SuccessPos: []int{5},
		}).ExecRelease(); err != nil {
			t.Fatal(err)
		}

		Print("And: we init table token range at position 1")
		if err := pm.Init(ctx, []*tableTokenRange{
			{
				Keyspace:   "k1",
				Table:      "t1",
				Pos:        5,
				StartToken: 0,
				EndToken:   10,
				Replicas:   []string{"h1", "h2"},
			},
		}); err != nil {
			t.Fatal(err)
		}

		Print("Then: initialized progress for both h1 and h2 should be complete")
		initProgress := []RunProgress{
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h1",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 1,
				Success:     1,
				Error:       0,
			},
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h2",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 1,
				Success:     1,
				Error:       0,
			},
		}
		resume := getProgress(&run, session)

		if diff := cmp.Diff(initProgress, resume, UUIDComparer(), progTrans); diff != "" {
			t.Fatal(diff)
		}
	})
}

func getProgress(run *Run, session gocqlx.Session) []RunProgress {
	var rp = make([]RunProgress, 0)

	if err := table.RepairRunProgress.SelectQuery(session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).SelectRelease(&rp); err != nil {
		panic(err)
	}

	return rp
}

func getJobExecutions(run *Run, session gocqlx.Session) []JobExecution {
	var jobs = make([]JobExecution, 0)

	if err := table.RepairJobExecution.SelectQuery(session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).SelectRelease(&jobs); err != nil {
		panic(err)
	}

	return jobs
}

func intervalComparer() cmp.Option {
	return cmp.Comparer(func(a, b interval) bool {
		endEqual := false
		if a.End == nil || b.End == nil {
			if a.End == b.End {
				endEqual = true
			} else {
				return false
			}
		} else if a.End.Truncate(time.Millisecond).Equal((*b.End).Truncate(time.Millisecond)) {
			endEqual = true
		}
		return a.Start.Truncate(time.Millisecond).Equal(b.Start.Truncate(time.Millisecond)) && endEqual
	})
}
