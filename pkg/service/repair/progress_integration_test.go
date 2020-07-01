// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/mermaid/pkg/schema/table"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func TestProgressManagerIntegration(t *testing.T) {
	run := &Run{
		ClusterID: uuid.NewTime(),
		TaskID:    uuid.NewTime(),
		ID:        uuid.NewTime(),
		StartTime: timeutc.Now(),
	}

	opts := cmp.Options{
		UUIDComparer(),
		NearTimeComparer(10 * time.Millisecond),
	}

	t.Run("start", func(t *testing.T) {
		var (
			session = gocqlx.NewSession(CreateSession(t))
		)

		ctx := context.Background()
		pu := newProgressManager(run, session)
		if err := pu.Init(ctx, []*tableTokenRange{
			{
				Keyspace:   "k1",
				Table:      "t1",
				StartToken: 0,
				EndToken:   10,
				Replicas:   []string{"h1", "h2"},
			},
		}); err != nil {
			t.Fatal(err)
		}
		Print("When: initialized progress manager received OnStartJob")
		if err := pu.OnStartJob(ctx, job{
			Host: "h1",
			Ranges: []*tableTokenRange{
				{
					Keyspace:   "k1",
					Table:      "t1",
					StartToken: 0,
					EndToken:   10,
					Replicas:   []string{"h1", "h2"},
				},
			}}); err != nil {
			t.Fatal(err)
		}
		now := timeutc.Now()

		Print("Then: progress is updated with startedAt timestamps")
		startProgress := []RunProgress{
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h1",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 1,
				Success:     0,
				Error:       0,
				StartedAt:   &now,
				CompletedAt: nil,
			}, {
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        "h2",
				Keyspace:    "k1",
				Table:       "t1",
				TokenRanges: 1,
				Success:     0,
				Error:       0,
				StartedAt:   &now,
				CompletedAt: nil,
			},
		}

		updates := getProgress(run, session)
		if diff := cmp.Diff(startProgress, updates, opts); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("update", func(t *testing.T) {
		var (
			session = gocqlx.NewSession(CreateSession(t))
		)

		ctx := context.Background()
		pu := newProgressManager(run, session)
		Print("When: run progress is initialized with incomplete values")
		if err := pu.Init(ctx, []*tableTokenRange{
			{
				Keyspace:   "k1",
				Table:      "t1",
				StartToken: 0,
				EndToken:   10,
				Replicas:   []string{"h1", "h2"},
			},
		}); err != nil {
			t.Fatal(err)
		}
		Print("When: update is called upon progress manager")
		if err := pu.Update(ctx, jobResult{
			job: job{
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
			},
			Err: nil,
		}); err != nil {
			t.Fatal(err)
		}
		now := timeutc.Now()

		Print("And: update progress is received with completed values")
		updateProgress := []RunProgress{
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
				StartedAt:   nil,
				CompletedAt: &now,
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
				StartedAt:   nil,
				CompletedAt: &now,
			},
		}
		Print("And: state is saved for completed range")
		stateProgress := []*RunState{
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
		updates := getProgress(run, session)
		states, err := pu.getState(run)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(updateProgress, updates, opts); diff != "" {
			t.Fatal(diff)
		}
		if diff := cmp.Diff(stateProgress, states, UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("restoring state", func(t *testing.T) {
		var (
			session = gocqlx.NewSession(CreateSession(t))
		)

		table.RepairRunState.Insert()

		ctx := context.Background()
		run := *run
		run.PrevID = uuid.NewTime()
		pm := newProgressManager(&run, session)

		Print("When: there is present state of success at position 1")
		if err := pm.upsertState(ctx, &RunState{
			ClusterID:  run.ClusterID,
			TaskID:     run.TaskID,
			RunID:      run.PrevID,
			Keyspace:   "k1",
			Table:      "t1",
			SuccessPos: []int{5},
		}); err != nil {
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
				StartedAt:   nil,
				CompletedAt: nil,
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
				StartedAt:   nil,
				CompletedAt: nil,
			},
		}
		resume := getProgress(&run, session)

		if diff := cmp.Diff(initProgress, resume, UUIDComparer(), progTrans); diff != "" {
			t.Fatal(diff)
		}
	})
}

func getProgress(run *Run, session gocqlx.Session) []RunProgress {
	stmt, names := table.RepairRunProgress.Select()

	q := session.Query(stmt, names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var rp = make([]RunProgress, 0)
	if err := q.SelectRelease(&rp); err != nil {

	}
	return rp
}
