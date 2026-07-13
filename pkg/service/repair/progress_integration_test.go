// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package repair

import (
	"context"
	"net/netip"
	"slices"
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

	h1 := netip.MustParseAddr("192.168.100.11")
	h2 := netip.MustParseAddr("192.168.100.12")

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
			p = &plan{
				Hosts: []string{h1.String(), h2.String()},
				Stats: map[scyllaclient.HostKeyspaceTable]tableStats{
					newHostKsTable(h1.String(), "k1", "t1"): {
						Size:   5,
						Ranges: 2,
					},
					newHostKsTable(h2.String(), "k1", "t1"): {
						Size:   7,
						Ranges: 2,
					},
				},
				Keyspaces: []keyspacePlan{
					{
						Keyspace: "k1",
						Tables:   []tablePlan{{Table: "t1"}},
					},
				},
			}
		)

		ctx := context.Background()
		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		prevID := uuid.Nil
		if prev := pm.GetPrevRun(ctx, 0); prev != nil {
			prevID = prev.ID
		}
		Print("When: run progress is initialized with incomplete values")
		if err := pm.Init(p, prevID); err != nil {
			t.Fatal(err)
		}

		Print("Then: progress rows are initialized with zero values")
		goldenProgress := []RunProgress{
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        h1.String(),
				Keyspace:    "k1",
				Table:       "t1",
				Size:        5,
				TokenRanges: 2,
				Success:     0,
				Error:       0,
			},
			{
				ClusterID:   run.ClusterID,
				TaskID:      run.TaskID,
				RunID:       run.ID,
				Host:        h2.String(),
				Keyspace:    "k1",
				Table:       "t1",
				Size:        7,
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
			master:     netip.MustParseAddr("192.168.100.11"),
			replicaSet: []netip.Addr{netip.MustParseAddr("192.168.100.11"), netip.MustParseAddr("192.168.100.12")},
			ranges:     []scyllaclient.TokenRange{token1},
		}

		start := timeutc.Now()
		pm.OnJobStart(ctx, j)
		Print("Then: run progress is updated with starting times")
		goldenProgress[0].StartedAt = &start
		goldenProgress[1].StartedAt = &start

		updatedProgress = getProgress(run, session)
		if diff := cmp.Diff(goldenProgress, updatedProgress, opts); diff != "" {
			t.Fatal(diff)
		}

		Print("When: OnJobEnd is called on progress manager")
		pm.OnJobEnd(ctx, jobResult{job: j})

		Print("Then: progress is updated with success")
		goldenProgress[0].Success = 1
		goldenProgress[1].Success = 1
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
			token3 = scyllaclient.TokenRange{
				StartToken: 50,
				EndToken:   100,
			}
			p = &plan{ // Plan containing token1 and token2
				Stats: map[scyllaclient.HostKeyspaceTable]tableStats{
					newHostKsTable(h1.String(), "k1", "t1"): {
						Ranges: 1,
					},
					newHostKsTable(h2.String(), "k1", "t1"): {
						Ranges: 1,
					},
				},
				Keyspaces: []keyspacePlan{
					{
						Keyspace: "k1",
						Tables:   []tablePlan{{Table: "t1", RangesCnt: 2}},
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
			Host:        h1.String(),
			Keyspace:    "k1",
			Table:       "t1",
			TokenRanges: 1,
			Success:     1,
		}).ExecRelease(); err != nil {
			t.Fatal(err)
		}
		if err := table.RepairRunProgress.InsertQuery(session).BindStruct(&RunProgress{
			ClusterID:   prevRun.ClusterID,
			TaskID:      prevRun.TaskID,
			RunID:       prevRun.ID,
			Host:        h2.String(),
			Keyspace:    "k1",
			Table:       "t1",
			TokenRanges: 1,
			Success:     1,
		}).ExecRelease(); err != nil {
			t.Fatal(err)
		}

		Print("And: we update plan")
		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		prevID := uuid.Nil
		if prev := pm.GetPrevRun(context.Background(), 0); prev != nil {
			prevID = prev.ID
		}
		if err := pm.Init(p, prevID); err != nil {
			t.Fatal(err)
		}
		done, all := pm.GetCompletedRanges("k1", "t1")
		Print("Then: validate marked token1 and not marked token3")
		if all != 2 || len(done) != 2 || !slices.Contains(done, token1) || !slices.Contains(done, token3) {
			t.Fatal("expected both token ranges to be done")
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

// TestRepairProgressDurationIntegration verifies that repair progress duration:
// a) Uses wall-clock time (CompletedAt - StartedAt) at every level (host+table, host, table, overall)
// b) Does not inherit duration from previous runs (fully/partially completed entries)
// c) Ignores legacy Duration/DurationStartedAt fields stored in DB
func TestRepairProgressDurationIntegration(t *testing.T) {
	h1 := netip.MustParseAddr("192.168.100.11")
	h2 := netip.MustParseAddr("192.168.100.12")

	t.Run("wall-clock duration at all levels", func(t *testing.T) {
		session := CreateScyllaManagerDBSession(t)
		run := &Run{
			ClusterID: uuid.NewTime(),
			TaskID:    uuid.NewTime(),
			ID:        uuid.NewTime(),
			StartTime: timeutc.Now(),
		}

		// Setup: insert RunProgress entries with known timestamps.
		// h1/k1/t1: 10:00:00 → 10:00:30 (30s)
		// h1/k1/t2: 10:00:10 → 10:00:40 (30s)
		// h2/k1/t1: 10:00:05 → 10:00:35 (30s)
		base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		ts := func(sec int) *time.Time {
			t := base.Add(time.Duration(sec) * time.Second)
			return &t
		}

		rps := []*RunProgress{
			{
				ClusterID: run.ClusterID, TaskID: run.TaskID, RunID: run.ID,
				Host: h1.String(), Keyspace: "k1", Table: "t1",
				Size: 10, TokenRanges: 5, Success: 5,
				StartedAt: ts(0), CompletedAt: ts(30),
			},
			{
				ClusterID: run.ClusterID, TaskID: run.TaskID, RunID: run.ID,
				Host: h1.String(), Keyspace: "k1", Table: "t2",
				Size: 6, TokenRanges: 3, Success: 3,
				StartedAt: ts(10), CompletedAt: ts(40),
			},
			{
				ClusterID: run.ClusterID, TaskID: run.TaskID, RunID: run.ID,
				Host: h2.String(), Keyspace: "k1", Table: "t1",
				Size: 10, TokenRanges: 5, Success: 5,
				StartedAt: ts(5), CompletedAt: ts(35),
			},
		}
		saveProgress(rps, session)

		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		p, err := pm.AggregateProgress()
		if err != nil {
			t.Fatal(err)
		}

		// Host+Table level: each entry uses its own wall-clock
		findHostTable := func(host, ks, tbl string) *TableProgress {
			for i := range p.Hosts {
				if p.Hosts[i].Host == host {
					for j := range p.Hosts[i].Tables {
						tp := &p.Hosts[i].Tables[j]
						if tp.Keyspace == ks && tp.Table == tbl {
							return tp
						}
					}
				}
			}
			return nil
		}

		if d := findHostTable(h1.String(), "k1", "t1").Duration; d != 30000 {
			t.Errorf("h1/k1/t1 duration: got %d, want 30000", d)
		}
		if d := findHostTable(h1.String(), "k1", "t2").Duration; d != 30000 {
			t.Errorf("h1/k1/t2 duration: got %d, want 30000", d)
		}
		if d := findHostTable(h2.String(), "k1", "t1").Duration; d != 30000 {
			t.Errorf("h2/k1/t1 duration: got %d, want 30000", d)
		}

		// Host level: wall-clock from min(StartedAt) to max(CompletedAt)
		findHost := func(host string) *HostProgress {
			for i := range p.Hosts {
				if p.Hosts[i].Host == host {
					return &p.Hosts[i]
				}
			}
			return nil
		}

		// h1: min(0,10)=0 → max(30,40)=40 → 40s
		if d := findHost(h1.String()).Duration; d != 40000 {
			t.Errorf("h1 duration: got %d, want 40000", d)
		}
		// h2: 5 → 35 → 30s
		if d := findHost(h2.String()).Duration; d != 30000 {
			t.Errorf("h2 duration: got %d, want 30000", d)
		}

		// Table level: wall-clock from min(StartedAt) to max(CompletedAt) across hosts
		findTable := func(ks, tbl string) *TableProgress {
			for i := range p.Tables {
				if p.Tables[i].Keyspace == ks && p.Tables[i].Table == tbl {
					return &p.Tables[i]
				}
			}
			return nil
		}

		// k1.t1: min(0,5)=0 → max(30,35)=35 → 35s
		if d := findTable("k1", "t1").Duration; d != 35000 {
			t.Errorf("k1/t1 duration: got %d, want 35000", d)
		}
		// k1.t2: 10 → 40 → 30s
		if d := findTable("k1", "t2").Duration; d != 30000 {
			t.Errorf("k1/t2 duration: got %d, want 30000", d)
		}

		// Overall: min(0,5,10)=0 → max(30,35,40)=40 → 40s
		if d := p.Duration; d != 40000 {
			t.Errorf("overall duration: got %d, want 40000", d)
		}
	})

	t.Run("resumed run does not inherit duration from previous run", func(t *testing.T) {
		session := CreateScyllaManagerDBSession(t)

		prevRun := &Run{
			ClusterID: uuid.NewTime(),
			TaskID:    uuid.NewTime(),
			ID:        uuid.NewTime(),
			StartTime: timeutc.Now(),
		}
		run := &Run{
			ClusterID: prevRun.ClusterID,
			TaskID:    prevRun.TaskID,
			ID:        uuid.NewTime(),
			StartTime: timeutc.Now(),
		}

		// Previous run progress:
		// h1/k1/t1: fully completed
		// h1/k1/t2: partially completed (1 of 2 ranges done)
		// h2/k1/t1: fully completed
		base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		prevStart := base
		prevEnd := base.Add(30 * time.Second)

		if err := table.RepairRun.InsertQuery(session).BindStruct(prevRun).Exec(); err != nil {
			t.Fatal(err)
		}
		prevProgress := []*RunProgress{
			{
				ClusterID: prevRun.ClusterID, TaskID: prevRun.TaskID, RunID: prevRun.ID,
				Host: h1.String(), Keyspace: "k1", Table: "t1",
				Size: 10, TokenRanges: 5, Success: 5,
				StartedAt: &prevStart, CompletedAt: &prevEnd,
			},
			{
				ClusterID: prevRun.ClusterID, TaskID: prevRun.TaskID, RunID: prevRun.ID,
				Host: h1.String(), Keyspace: "k1", Table: "t2",
				Size: 6, TokenRanges: 2, Success: 1,
				StartedAt: &prevStart,
			},
			{
				ClusterID: prevRun.ClusterID, TaskID: prevRun.TaskID, RunID: prevRun.ID,
				Host: h2.String(), Keyspace: "k1", Table: "t1",
				Size: 10, TokenRanges: 5, Success: 5,
				StartedAt: &prevStart, CompletedAt: &prevEnd,
			},
		}
		saveProgress(prevProgress, session)

		// Save previous run state (h1/k1/t1 and h2/k1/t1 fully done)
		for _, rs := range []*RunState{
			{
				ClusterID: prevRun.ClusterID, TaskID: prevRun.TaskID, RunID: prevRun.ID,
				Keyspace: "k1", Table: "t1",
				SuccessRanges: []scyllaclient.TokenRange{{StartToken: 0, EndToken: 100}},
			},
			{
				ClusterID: prevRun.ClusterID, TaskID: prevRun.TaskID, RunID: prevRun.ID,
				Keyspace: "k1", Table: "t2",
				SuccessRanges: []scyllaclient.TokenRange{{StartToken: 0, EndToken: 50}},
			},
		} {
			if err := table.RepairRunState.InsertQuery(session).BindStruct(rs).ExecRelease(); err != nil {
				t.Fatal(err)
			}
		}

		// Init current run with plan covering same tables
		p := &plan{
			Hosts: []string{h1.String(), h2.String()},
			Stats: map[scyllaclient.HostKeyspaceTable]tableStats{
				newHostKsTable(h1.String(), "k1", "t1"): {Size: 10, Ranges: 5},
				newHostKsTable(h1.String(), "k1", "t2"): {Size: 6, Ranges: 2},
				newHostKsTable(h2.String(), "k1", "t1"): {Size: 10, Ranges: 5},
			},
			Keyspaces: []keyspacePlan{
				{
					Keyspace: "k1",
					Tables:   []tablePlan{{Table: "t1", RangesCnt: 5}, {Table: "t2", RangesCnt: 2}},
				},
			},
		}

		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.Init(p, prevRun.ID); err != nil {
			t.Fatal(err)
		}

		// Simulate job for h1/k1/t2 (the partially completed entry)
		ctx := context.Background()
		j := job{
			keyspace:   "k1",
			table:      "t2",
			master:     h1,
			replicaSet: []netip.Addr{h1},
			ranges:     []scyllaclient.TokenRange{{StartToken: 50, EndToken: 100}},
		}
		pm.OnJobStart(ctx, j)
		time.Sleep(10 * time.Millisecond)
		pm.OnJobEnd(ctx, jobResult{job: j})

		// Aggregate and verify
		prog, err := pm.AggregateProgress()
		if err != nil {
			t.Fatal(err)
		}

		findHostTable := func(host, ks, tbl string) *TableProgress {
			for i := range prog.Hosts {
				if prog.Hosts[i].Host == host {
					for j := range prog.Hosts[i].Tables {
						tp := &prog.Hosts[i].Tables[j]
						if tp.Keyspace == ks && tp.Table == tbl {
							return tp
						}
					}
				}
			}
			return nil
		}

		// h1/k1/t1: fully done in prev run, no work this run → duration 0, nil timestamps
		ht := findHostTable(h1.String(), "k1", "t1")
		if ht.Duration != 0 {
			t.Errorf("h1/k1/t1 (done in prev run) duration: got %d, want 0", ht.Duration)
		}
		if ht.StartedAt != nil {
			t.Errorf("h1/k1/t1 StartedAt should be nil, got %v", ht.StartedAt)
		}

		// h2/k1/t1: fully done in prev run → duration 0
		ht = findHostTable(h2.String(), "k1", "t1")
		if ht.Duration != 0 {
			t.Errorf("h2/k1/t1 (done in prev run) duration: got %d, want 0", ht.Duration)
		}

		// h1/k1/t2: had work this run → duration > 0, reflecting current run only
		ht = findHostTable(h1.String(), "k1", "t2")
		if ht.Duration <= 0 {
			t.Errorf("h1/k1/t2 (retried this run) duration: got %d, want > 0", ht.Duration)
		}
		// Duration should be small (around 10ms), not 30s from previous run
		if ht.Duration > 5000 {
			t.Errorf("h1/k1/t2 duration too large (inherited from prev run?): got %dms", ht.Duration)
		}

		// Host h2: all entries done from prev run → duration 0
		findHost := func(host string) *HostProgress {
			for i := range prog.Hosts {
				if prog.Hosts[i].Host == host {
					return &prog.Hosts[i]
				}
			}
			return nil
		}
		if d := findHost(h2.String()).Duration; d != 0 {
			t.Errorf("h2 (all done in prev run) duration: got %d, want 0", d)
		}
	})

	t.Run("legacy Duration and DurationStartedAt fields in DB are ignored", func(t *testing.T) {
		session := CreateScyllaManagerDBSession(t)
		run := &Run{
			ClusterID: uuid.NewTime(),
			TaskID:    uuid.NewTime(),
			ID:        uuid.NewTime(),
			StartTime: timeutc.Now(),
		}

		// Insert entry with large legacy Duration field but known wall-clock timestamps
		base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		start := base
		end := base.Add(30 * time.Second)
		legacyDurationStart := base.Add(-1 * time.Hour)

		rps := []*RunProgress{
			{
				ClusterID: run.ClusterID, TaskID: run.TaskID, RunID: run.ID,
				Host: h1.String(), Keyspace: "k1", Table: "t1",
				Size: 10, TokenRanges: 5, Success: 5,
				StartedAt: &start, CompletedAt: &end,
				Duration:          999 * time.Second,
				DurationStartedAt: &legacyDurationStart,
			},
		}
		saveProgress(rps, session)

		pm := NewDBProgressManager(run, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		p, err := pm.AggregateProgress()
		if err != nil {
			t.Fatal(err)
		}

		// Duration should be 30s (wall-clock), not 999s (legacy field)
		if p.Duration != 30000 {
			t.Errorf("overall duration: got %d, want 30000 (legacy Duration field should be ignored)", p.Duration)
		}
		if p.Hosts[0].Duration != 30000 {
			t.Errorf("host duration: got %d, want 30000", p.Hosts[0].Duration)
		}
	})
}

func getProgress(run *Run, session gocqlx.Session) []RunProgress {
	rp := make([]RunProgress, 0)

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
	rs := make([]*RunState, 0)

	if err := table.RepairRunState.SelectQuery(session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).SelectRelease(&rs); err != nil {
		panic(err)
	}

	return rs
}
