// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	s, err := repair.NewService(
		mermaidtest.CreateSession(t),
		func(uuid.UUID) (*scylla.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopment().Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

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

	t.Run("get missing unit", func(t *testing.T) {
		t.Parallel()

		u, err := s.GetUnit(ctx, uuid.MustRandom(), uuid.MustRandom())
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
		v := u0.ID

		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}
		if u0.ID == v {
			t.Fatal("ID not updated")
		}
		u1, err := s.GetUnit(ctx, u0.ClusterID, u0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(u0, u1, mermaidtest.UUIDComparer()); diff != "" {
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
		_, err := s.GetUnit(ctx, u.ClusterID, u.ID)
		if err != mermaid.ErrNotFound {
			t.Fatal("expected nil")
		}
	})

	t.Run("list empty units", func(t *testing.T) {
		t.Parallel()

		units, err := s.ListUnits(ctx, uuid.MustRandom())
		if err != nil {
			t.Fatal(err)
		}
		if len(units) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list units", func(t *testing.T) {
		t.Parallel()

		expected := make([]*repair.Unit, 3)
		u0 := validUnit()
		ctx := context.Background()
		for i := range expected {
			u := &repair.Unit{
				ID:        uuid.NewTime(),
				ClusterID: u0.ClusterID,
				Keyspace:  "keyspace" + strconv.Itoa(i),
				Tables: []string{
					"table" + strconv.Itoa(2*i),
					"table" + strconv.Itoa(2*i+1),
				},
			}
			if err := s.PutUnit(ctx, u); err != nil {
				t.Fatal(err)
			}
			expected[i] = u
		}

		units, err := s.ListUnits(ctx, u0.ClusterID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(units, expected, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
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
	uuid := uuid.MustRandom()

	return &repair.Unit{
		ClusterID: uuid,
		Keyspace:  "keyspace",
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	createKeyspace(t, session, "test_repair")
	createTable(t, session, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	//l := log.NewDevelopment()
	l := prodLogger(t)
	s, err := repair.NewService(
		session,
		func(uuid.UUID) (*scylla.Client, error) {
			c, err := scylla.NewClient(mermaidtest.ClusterHosts, l.Named("scylla"))
			if err != nil {
				return nil, err
			}
			config := scylla.Config{
				"murmur3_partitioner_ignore_msb_bits": float64(12),
				"shard_count":                         float64(2),
			}
			return scylla.WithConfig(c, config), nil
		},
		l.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var (
		clusterID = uuid.MustRandom()
		taskID    = uuid.NewTime()
		ctx       = context.Background()
	)

	// put a unit
	u := repair.Unit{
		ClusterID: clusterID,
		Keyspace:  "test_repair",
	}
	if err := s.PutUnit(ctx, &u); err != nil {
		t.Fatal(err)
	}

	// repair
	if err := s.Repair(ctx, &u, taskID); err != nil {
		t.Fatal(err)
	}

	r, err := s.GetRun(ctx, &u, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != repair.StatusRunning {
		t.Fatal("wrong status", r)
	}

	// wait
	time.Sleep(5 * time.Second)

	// check ongoing progress
	p, err := s.GetProgress(ctx, &u, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 2+2 {
		t.Fatalf("%+v", p)
	}

	totalDone := 0
	for _, v := range p {
		totalDone += v.SegmentSuccess + v.SegmentError
	}
	if totalDone == 0 {
		t.Fatalf("%+v", p)
	}
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	var q *gocqlx.Queryx

	q = gocqlx.Query(session.Query("DROP KEYSPACE IF EXISTS "+keyspace), nil)
	if err := q.ExecRelease(); err != nil {
		t.Fatal(err)
	}

	q = gocqlx.Query(session.Query("CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};"), nil)
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

func prodLogger(t *testing.T) log.Logger {
	z, err := log.NewProduction("integration")
	if err != nil {
		t.Fatal(err)
	}
	return z
}
