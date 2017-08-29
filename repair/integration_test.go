// +build all integration

package repair_test

import (
	"context"
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
)

func TestService(t *testing.T) {
	s, err := repair.NewService(mermaidtest.CreateSession(t), log.NewDevelopmentLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("GetGlobalMergedUnitConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

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
		id, _ := gocql.RandomUUID()

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
		id, _ := gocql.RandomUUID()

		invalid := math.MaxInt64
		c := validConfig()
		c.SegmentsPerShard = &invalid

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, c); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("PutNilConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		if err := s.PutConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"}, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("DeleteMissingConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		err := s.DeleteConfig(ctx, repair.ConfigSource{id, repair.UnitConfig, "id"})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PutAndGetConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

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
		id, _ := gocql.RandomUUID()

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
		id, _ := gocql.RandomUUID()

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
		u.ID = mermaid.UUIDFromUint64(0, 1)

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
		id, _ := gocql.RandomUUID()

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
	segmentsPerShard := 50
	retryLimit := 3
	retryBackoffSeconds := 60
	parallelNodeLimit := -1
	parallelShardPercent := float32(1)

	return &repair.Config{
		Enabled:              &enabled,
		SegmentsPerShard:     &segmentsPerShard,
		RetryLimit:           &retryLimit,
		RetryBackoffSeconds:  &retryBackoffSeconds,
		ParallelNodeLimit:    &parallelNodeLimit,
		ParallelShardPercent: &parallelShardPercent,
	}
}

func validUnit() *repair.Unit {
	uuid, _ := gocql.RandomUUID()

	return &repair.Unit{
		ClusterID: uuid,
		Keyspace:  "keyspace",
	}
}
