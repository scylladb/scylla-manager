// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	s, err := repair.NewService(
		session,
		func(context.Context, uuid.UUID) (*scylla.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopment().Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	putRun := func(t *testing.T, r *repair.Run) {
		stmt, names := schema.RepairRun.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("get global merged unit config", func(t *testing.T) {
		t.Parallel()

		c, err := s.GetMergedUnitConfig(ctx, &repair.Unit{
			ID:        uuid.MustRandom(),
			ClusterID: uuid.MustRandom(),
			Keyspace:  "keyspace",
		})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(&c.Config, validConfig()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing config", func(t *testing.T) {
		t.Parallel()

		c, err := s.GetConfig(ctx, repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		})
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("put nil config", func(t *testing.T) {
		t.Parallel()

		if err := s.PutConfig(ctx, repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put invalid config", func(t *testing.T) {
		t.Parallel()

		invalid := -1
		c := validConfig()
		c.RetryLimit = &invalid

		if err := s.PutConfig(ctx, repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}, c); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("delete missing config", func(t *testing.T) {
		t.Parallel()

		if err := s.DeleteConfig(ctx, repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("put and get config", func(t *testing.T) {
		t.Parallel()

		src := repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}

		c0 := validConfig()
		c0.RetryLimit = nil
		c0.RetryBackoffSeconds = nil

		if err := s.PutConfig(ctx, src, c0); err != nil {
			t.Fatal(err)
		}
		c1, err := s.GetConfig(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c1); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("put and delete config", func(t *testing.T) {
		t.Parallel()

		src := repair.ConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}

		c := validConfig()

		if err := s.PutConfig(ctx, src, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteConfig(ctx, src); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetConfig(ctx, src)
		if err != mermaid.ErrNotFound {
			t.Fatal("expected nil")
		}
	})

	t.Run("list empty units", func(t *testing.T) {
		t.Parallel()

		units, err := s.ListUnits(ctx, uuid.MustRandom(), &repair.UnitFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(units) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list units", func(t *testing.T) {
		t.Parallel()

		id := uuid.MustRandom()

		expected := make([]*repair.Unit, 3)
		for i := range expected {
			u := &repair.Unit{
				ClusterID: id,
				ID:        uuid.NewTime(),
				Keyspace:  "keyspace" + strconv.Itoa(i),
				Tables: []string{
					fmt.Sprintf("table%d", 2*i),
					fmt.Sprintf("table%d", 2*i+1),
				},
			}
			if err := s.PutUnit(ctx, u); err != nil {
				t.Fatal(err)
			}
			expected[i] = u
		}

		units, err := s.ListUnits(ctx, id, &repair.UnitFilter{})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(units, expected, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing unit", func(t *testing.T) {
		t.Parallel()

		u, err := s.GetUnitByID(ctx, uuid.MustRandom(), uuid.MustRandom())
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if u != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("get unit", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()
		u0.ID = uuid.Nil

		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}
		if u0.ID == uuid.Nil {
			t.Fatal("ID not updated")
		}
		u1, err := s.GetUnitByID(ctx, u0.ClusterID, u0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(u0, u1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}

		u2, err := s.GetUnitByName(ctx, u0.ClusterID, u0.Name)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(u0, u2, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("put nil unit", func(t *testing.T) {
		t.Parallel()

		if err := s.PutUnit(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put invalid unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		u.ClusterID = uuid.Nil

		if err := s.PutUnit(ctx, u); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put conflicting unit", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()

		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}

		u1 := u0
		u1.ID = uuid.Nil

		if err := s.PutUnit(ctx, u0); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put new unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		u.ID = uuid.Nil

		if err := s.PutUnit(ctx, u); err != nil {
			t.Fatal(err)
		}
		if u.ID == uuid.Nil {
			t.Fatal("id not set")
		}
	})

	t.Run("delete missing unit", func(t *testing.T) {
		t.Parallel()

		err := s.DeleteUnit(ctx, uuid.MustRandom(), uuid.MustRandom())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		if err := s.PutUnit(ctx, u); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteUnit(ctx, u.ClusterID, u.ID); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetUnitByID(ctx, u.ClusterID, u.ID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("list runs invalid filter", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		_, err := s.ListRuns(ctx, u, nil)
		if err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("list runs", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		table := []struct {
			F *repair.RunFilter
			E []*repair.Run
		}{
			// All runs
			{
				F: &repair.RunFilter{},
				E: []*repair.Run{r1, r0},
			},
			// Add limit
			{
				F: &repair.RunFilter{Limit: 1},
				E: []*repair.Run{r1},
			},
		}

		for _, test := range table {
			runs, err := s.ListRuns(ctx, u, test.F)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(runs, test.E, mermaidtest.UUIDComparer()); diff != "" {
				t.Fatal(diff)
			}
		}
	})

	t.Run("get last run", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		r, err := s.GetLastRun(ctx, u)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("stop run", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u.ID,
			ClusterID: u.ClusterID,
			Status:    repair.StatusRunning,
		}

		putRun(t, &r)

		if err := s.StopRun(ctx, u, r.ID); err != nil {
			t.Fatal(err)
		}

		if run, err := s.GetRun(ctx, u, r.ID); err != nil {
			t.Fatal(err)
		} else if run.Status != repair.StatusStopping {
			t.Fatal(run.Status)
		}
	})

	t.Run("fix run status", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()
		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}

		u1 := validUnit()
		if err := s.PutUnit(ctx, u1); err != nil {
			t.Fatal(err)
		}

		r0 := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u0.ID,
			ClusterID: u0.ClusterID,
			Status:    repair.StatusRunning,
		}
		putRun(t, &r0)

		r1 := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u1.ID,
			ClusterID: u1.ClusterID,
			Status:    repair.StatusStopping,
		}
		putRun(t, &r1)

		if err := s.FixRunStatus(ctx); err != nil {
			t.Fatal(err)
		}

		if r, err := s.GetRun(ctx, u0, r0.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}

		if r, err := s.GetRun(ctx, u1, r1.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}
	})
}

func validConfig() *repair.Config {
	enabled := true
	segmentSizeLimit := int64(-1)
	retryLimit := 3
	retryBackoffSeconds := 60
	parallelShardPercent := float32(1)

	return &repair.Config{
		Enabled:              &enabled,
		SegmentSizeLimit:     &segmentSizeLimit,
		RetryLimit:           &retryLimit,
		RetryBackoffSeconds:  &retryBackoffSeconds,
		ParallelShardPercent: &parallelShardPercent,
	}
}

func validUnit() *repair.Unit {
	return &repair.Unit{
		ClusterID: uuid.MustRandom(),
		ID:        uuid.MustRandom(),
		Name:      "name",
		Keyspace:  "keyspace",
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	createKeyspace(t, session, "test_repair")
	createTable(t, session, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	var (
		s            = newTestService(t, session)
		clusterID    = uuid.MustRandom()
		taskID       = uuid.NewTime()
		unit         = repair.Unit{ClusterID: clusterID, Keyspace: "test_repair"}
		segmentsDone = 0
		ctx          = context.Background()
	)

	assertStatus := func(expected repair.Status) {
		if r, err := s.GetRun(ctx, &unit, taskID); err != nil {
			t.Fatal(err)
		} else if r.Status != expected {
			t.Fatal("wrong status", r, "expected", expected)
		}
	}

	assertProgress := func() {
		prog, err := s.GetProgress(ctx, &unit, taskID)
		if err != nil {
			t.Fatal(err)
		}
		if len(prog) != 4 {
			t.Fatal()
		}

		done := 0
		for _, p := range prog {
			done += p.SegmentSuccess + p.SegmentError
		}

		if done < segmentsDone {
			t.Fatal("no progress, got", done, "had", segmentsDone)
		}

		segmentsDone = done
	}

	wait := func() {
		time.Sleep(5 * time.Second)
	}

	waitHostProgress := func(host string, percent float64) {
		hostProgress := func(host string) (done, total int) {
			prog, err := s.GetProgress(ctx, &unit, taskID, host)
			if err != nil {
				t.Fatal(err)
			}

			for _, p := range prog {
				done += p.SegmentSuccess + p.SegmentError
				total += p.SegmentCount
			}
			return
		}

		for {
			done, total := hostProgress(host)
			if done >= int(float64(total)*percent) {
				break
			} else {
				t.Log(done, total)
				wait()
			}
		}
	}

	// Given unit
	if err := s.PutUnit(ctx, &unit); err != nil {
		t.Fatal(err)
	}

	// When run repair
	if err := s.Repair(ctx, &unit, taskID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair advances
	assertProgress()

	// When run another repair
	// Then run fails
	if err := s.Repair(ctx, &unit, uuid.NewTime()); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), taskID.String()) {
		t.Fatal(err)
	}

	// When stop run
	if err := s.StopRun(ctx, &unit, taskID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusStopping)

	// When wait
	wait()

	// Then status is StatusStopped
	assertStatus(repair.StatusStopped)

	// When create a new task
	taskID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, &unit, taskID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// And repair advances
	assertProgress()

	// When wait
	wait()

	// Then repair advances
	assertProgress()

	// When host is 1/2 repaired
	waitHostProgress("172.16.1.10", 0.5)

	// And close
	s.Close(ctx)

	// And wait
	wait()

	// And restart
	s = newTestService(t, session)
	s.FixRunStatus(ctx)

	// And create a new task
	taskID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, &unit, taskID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// And repair advances
	assertProgress()

	// When host is repaired
	waitHostProgress("172.16.1.10", 1)

	// Then wait
	wait()
}

func newTestService(t *testing.T, session *gocql.Session) *repair.Service {
	logger := log.NewDevelopment()

	s, err := repair.NewService(
		session,
		func(context.Context, uuid.UUID) (*scylla.Client, error) {
			c, err := scylla.NewClient(mermaidtest.ClusterHosts, logger.Named("scylla"))
			if err != nil {
				return nil, err
			}
			config := scylla.Config{
				"murmur3_partitioner_ignore_msb_bits": float64(12),
				"shard_count":                         float64(2),
			}
			return scylla.WithConfig(c, config), nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	var q *gocqlx.Queryx

	q = gocqlx.Query(session.Query("DROP KEYSPACE IF EXISTS "+keyspace), nil)
	if err := q.ExecRelease(); err != nil {
		t.Fatal(err)
	}

	q = gocqlx.Query(session.Query("CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}"), nil)
	if err := q.ExecRelease(); err != nil {
		t.Fatal(err)
	}
}

func createTable(t *testing.T, s *gocql.Session, table string) error {
	if err := s.Query(table).RetryPolicy(nil).Exec(); err != nil {
		t.Logf("error creating table table=%q err=%v\n", table, err)
		return err
	}

	return nil
}
