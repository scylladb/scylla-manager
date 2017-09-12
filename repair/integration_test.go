// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/dbapi"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	s, err := repair.NewService(
		mermaidtest.CreateSession(t),
		func(uuid.UUID) (*dbapi.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopmentLogger().Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("GetGlobalMergedUnitConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		v, err := s.GetMergedUnitConfig(ctx, &repair.Unit{ID: id, ClusterID: id, Keyspace: "keyspace"})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(&v.Config, validConfig()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("GetMissingConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		c, err := s.GetConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"})
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("PutInvalidConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		invalid := -1
		c := validConfig()
		c.RetryLimit = &invalid

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, c); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("PutNilConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("DeleteMissingConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		err := s.DeleteConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PutAndGetConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		c := validConfig()
		c.RetryLimit = nil
		c.RetryBackoffSeconds = nil

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, c); err != nil {
			t.Fatal(err)
		}
		actual, err := s.GetConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(actual, c); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("PutAndDeleteConfig", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		c := validConfig()

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"})
		if err != mermaid.ErrNotFound {
			t.Fatal("expected nil")
		}
	})

	t.Run("GetMissingUnit", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		u, err := s.GetUnit(ctx, id, id)
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if u != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("PutInvalidUnit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		u.ID = uuid.Nil

		if err := s.PutUnit(ctx, u); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("PutNilUnit", func(t *testing.T) {
		t.Parallel()

		if err := s.PutUnit(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("DeleteMissingUnit", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustRandom()

		err := s.DeleteUnit(ctx, id, id)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PutAndGetUnit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		v := u.ID

		if err := s.PutUnit(ctx, u); err != nil {
			t.Fatal(err)
		}
		if u.ID == v {
			t.Fatal("ID not updated")
		}
		actual, err := s.GetUnit(ctx, u.ClusterID, u.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(actual, u); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("PutAndDeleteUnit", func(t *testing.T) {
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
	createKeyspace(t, session, "repair_test")
	createTable(t, session, "CREATE TABLE repair_test.test (id int PRIMARY KEY)")

	l := log.NewDevelopmentLogger()
	s, err := repair.NewService(
		session,
		func(uuid.UUID) (*dbapi.Client, error) {
			c, err := dbapi.NewClient(mermaidtest.ClusterHosts, l.Named("dbapi"))
			if err != nil {
				return nil, err
			}
			config := dbapi.Config{
				"murmur3_partitioner_ignore_msb_bits": float64(12),
				"shard_count":                         float64(8),
			}
			return dbapi.WithConfig(c, config), nil
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
		Keyspace:  "repair_test",
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

	p, err := s.GetProgress(ctx, &u, taskID)
	if err != nil {
		t.Fatal(err)
	}
	l.Debug(ctx, "Progress", "Progress", p)
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
