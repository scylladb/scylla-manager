// Copyright (C) 2017 ScyllaDB

//go:build all || integration
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
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestProgressManagerIntegration(t *testing.T) {
	opts := cmp.Options{
		cmpopts.IgnoreUnexported(RunProgress{}),
		UUIDComparer(),
		NearTimeComparer(5 * time.Millisecond),
		NearDurationComparer(5 * time.Millisecond),
	}

	t.Run("progress update sequence (Init,OnJobStart,OnJobEnd)", func(t *testing.T) {
		var (
			run = &Run{
				ClusterID: uuid.NewTime(),
				TaskID:    uuid.NewTime(),
				ID:        uuid.NewTime(),
				StartTime: timeutc.Now(),
			}

			session = CreateScyllaManagerDBSession(t)
			token1  = scyllaclient.TokenRange{
				StartToken: 0,
				EndToken:   10,
			}
			token2 = scyllaclient.TokenRange{
				StartToken: 11,
				EndToken:   20,
			}
			p = &plan{
				Keyspaces: []keyspacePlan{
					{
						Keyspace: "k1",
						Tables: []tablePlan{
							{
								Table:           "t1",
								MarkedRanges:    make(map[scyllaclient.TokenRange]struct{}),
								MarkedInReplica: make([]int, 1),
							},
						},
						Replicas: []scyllaclient.ReplicaTokenRanges{
							{
								ReplicaSet: []string{"h1", "h2"},
								Ranges:     []scyllaclient.TokenRange{token1, token2},
							},
						},
						TokenRepIdx: map[scyllaclient.TokenRange]int{
							token1: 0,
							token2: 0,
						},
					},
				},
			}
		)

		ctx := context.Background()
		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.SetPrevRunID(ctx, 0); err != nil {
			t.Fatal(err)
		}
		Print("When: run progress is initialized with incomplete values")
		if err := pm.Init(p); err != nil {
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
		j := job{
			keyspace:   "k1",
			table:      "t1",
			master:     "h1",
			replicaSet: []string{"h1", "h2"},
			ranges:     []scyllaclient.TokenRange{token1},
		}

		start := timeutc.Now()
		pm.OnJobStart(ctx, j)
		Print("Then: run progress is updated with starting times")
		goldenProgress[0].StartedAt = &start
		goldenProgress[0].DurationStartedAt = &start
		goldenProgress[1].StartedAt = &start
		goldenProgress[1].DurationStartedAt = &start

		updatedProgress = getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		end := timeutc.Now()
		Print("When: OnJobEnd is called on progress manager")
		pm.OnJobEnd(ctx, jobResult{job: j})

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
				ClusterID:     run.ClusterID,
				TaskID:        run.TaskID,
				RunID:         run.ID,
				Keyspace:      "k1",
				Table:         "t1",
				SuccessRanges: []scyllaclient.TokenRange{token1},
			},
		}
		states := getState(run, session)
		if diff := cmp.Diff(goldenState, states, UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("restoring state", func(t *testing.T) {
		var (
			prevRun = &Run{
				ClusterID: uuid.NewTime(),
				TaskID:    uuid.NewTime(),
				ID:        uuid.NewTime(),
				StartTime: timeutc.Now(),
			}
			run = &Run{
				ClusterID: prevRun.ClusterID,
				TaskID:    prevRun.TaskID,
				ID:        uuid.NewTime(),
				StartTime: timeutc.Now(),
			}

			session = CreateScyllaManagerDBSession(t)
			token1  = scyllaclient.TokenRange{
				StartToken: 5,
				EndToken:   10,
			}
			token2 = scyllaclient.TokenRange{
				StartToken: 15,
				EndToken:   30,
			}
			token3 = scyllaclient.TokenRange{
				StartToken: 50,
				EndToken:   100,
			}
			p = &plan{ // Plan containing token1 and token2
				Keyspaces: []keyspacePlan{
					{
						Keyspace: "k1",
						Tables: []tablePlan{
							{
								Table:           "t1",
								MarkedRanges:    make(map[scyllaclient.TokenRange]struct{}),
								MarkedInReplica: make([]int, 1),
							},
						},
						Replicas: []scyllaclient.ReplicaTokenRanges{
							{
								ReplicaSet: []string{"h1", "h2"},
								Ranges:     []scyllaclient.TokenRange{token1},
							},
						},
						TokenRepIdx: map[scyllaclient.TokenRange]int{
							token1: 0,
							token2: 0,
						},
					},
				},
			}
		)

		Print("When: there are present success ranges token1, token3")
		// Fill all run, run state and run progress as progress manager takes
		// all of them into consideration when resuming previous run.
		if err := table.RepairRun.InsertQuery(session).BindStruct(&prevRun).Exec(); err != nil {
			t.Fatal(err)
		}
		if err := table.RepairRunState.InsertQuery(session).BindStruct(&RunState{
			ClusterID:     prevRun.ClusterID,
			TaskID:        prevRun.TaskID,
			RunID:         prevRun.ID,
			Keyspace:      "k1",
			Table:         "t1",
			SuccessRanges: []scyllaclient.TokenRange{token1, token3},
		}).ExecRelease(); err != nil {
			t.Fatal(err)
		}
		if err := table.RepairRunProgress.InsertQuery(session).BindStruct(&RunProgress{
			ClusterID:   prevRun.ClusterID,
			TaskID:      prevRun.TaskID,
			RunID:       prevRun.ID,
			Host:        "h1",
			Keyspace:    "k1",
			Table:       "t1",
			TokenRanges: 3,
			Success:     2,
		}).ExecRelease(); err != nil {
			t.Fatal(err)
		}

		Print("And: we update plan")
		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.SetPrevRunID(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		if err := pm.Init(p); err != nil {
			t.Fatal(err)
		}
		pm.UpdatePlan(p)

		Print("Then: validate marked token1 and not marked token3")
		tp := p.Keyspaces[0].Tables[0]
		if len(tp.MarkedRanges) != 1 {
			t.Fatal("expected 1 marked range")
		}
		if _, ok := tp.MarkedRanges[token1]; !ok {
			t.Fatal("expected token1 to be marked")
		}
		if tp.MarkedInReplica[0] != 1 {
			t.Fatal("expected 1 marked range in the first replica set")
		}
	})
}

func TestAggregateProgressIntegration(t *testing.T) {
	// Test names
	testNames := []string{
		"empty progress list",
		"multiple progress multi host",
		"single progress single host",
		"weighted progress",
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Progress{}, Unit{}, HostProgress{}, TableProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for _, name := range testNames {
		t.Run(name, func(t *testing.T) {
			session := CreateScyllaManagerDBSession(t)
			run := &Run{
				ClusterID: uuid.NewTime(),
				TaskID:    uuid.NewTime(),
				ID:        uuid.NewTime(),
				StartTime: timeutc.Now(),
			}

			var v []*RunProgress
			ReadInputJSONFile(t, &v)
			for _, rp := range v {
				rp.ClusterID = run.ClusterID
				rp.TaskID = run.TaskID
				rp.RunID = run.ID
			}
			saveProgress(v, session)

			pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
			res, err := pm.AggregateProgress()
			if err != nil {
				t.Error(err)
			}
			res.MaxIntensity = 777
			res.Intensity = 666
			res.MaxParallel = 99
			res.Parallel = 6

			var golden Progress
			SaveGoldenJSONFileIfNeeded(t, res)
			LoadGoldenJSONFile(t, &golden)
			if diff := cmp.Diff(golden, res, opts); diff != "" {
				t.Error(name, diff)
			}
		})
	}
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

func saveProgress(rps []*RunProgress, session gocqlx.Session) {
	q := table.RepairRunProgress.InsertQuery(session)
	defer q.Release()

	for _, rp := range rps {
		if err := q.BindStruct(rp).Exec(); err != nil {
			panic(err)
		}
	}
}

func getState(run *Run, session gocqlx.Session) []*RunState {
	var rs = make([]*RunState, 0)

	if err := table.RepairRunState.SelectQuery(session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).SelectRelease(&rs); err != nil {
		panic(err)
	}

	return rs
}
