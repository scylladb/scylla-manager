// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/metrics"
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
		cmpopts.IgnoreUnexported(RunProgress{}),
		UUIDComparer(),
		NearTimeComparer(5 * time.Millisecond),
		NearDurationComparer(1 * time.Millisecond),
	}

	t.Run("progress update sequence (Init,OnScyllaJobStart,OnScyllaJobEnd,OnJobResult,CheckRepaired)", func(t *testing.T) {
		var (
			session = CreateSession(t)
			ttrs    = []*tableTokenRange{
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
			}
		)

		ctx := context.Background()
		pm := newProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		Print("When: run progress is initialized with incomplete values")
		if err := pm.Init(ctx, ttrs); err != nil {
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

		Print("When: OnScyllaJobStart called on progress manager")
		var jobID int32 = 1
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
		if err := pm.OnScyllaJobStart(ctx, j, jobID); err != nil {
			t.Fatal(err)
		}
		Print("Then: run progress is updated with starting times")
		goldenProgress[0].StartedAt = &start
		goldenProgress[0].DurationStartedAt = &start
		goldenProgress[1].StartedAt = &start
		goldenProgress[1].DurationStartedAt = &start

		updatedProgress = getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnScyllaJobEnd is called on progress manager")
		end := timeutc.Now()
		if err := pm.OnScyllaJobEnd(ctx, j, jobID); err != nil {
			t.Fatal(err)
		}

		Print("Then: there are no changes in the database")
		updatedProgress = getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnJobResult is called on progress manager")
		if err := pm.OnJobResult(ctx, jobResult{
			job: j,
			Err: nil,
		}); err != nil {
			t.Fatal(err)
		}

		Print("Then: progress is updated with success and duration")
		goldenProgress[0].Success = 1
		goldenProgress[1].Success = 1
		goldenProgress[0].Duration = end.Sub(start)
		goldenProgress[1].Duration = end.Sub(start)
		goldenProgress[0].DurationStartedAt = nil
		goldenProgress[1].DurationStartedAt = nil
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
		if pm.CheckRepaired(ttrs[1]) {
			t.Error("Expected ranges to be not repaired")
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
		pm := newProgressManager(&run, session, metrics.NewRepairMetrics(), log.NewDevelopment())

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

		if diff := cmp.Diff(initProgress, resume, opts, progTrans); diff != "" {
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
