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

	t.Run("GetMissingConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		c, err := s.GetConfig(ctx, id, repair.UnitConfig, "id")
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
		config := validConfig()
		config.SegmentsPerShard = &invalid

		if err := s.PutConfig(ctx, id, repair.UnitConfig, "id", config); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("PutNilConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		if err := s.PutConfig(ctx, id, repair.UnitConfig, "id", nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("DeleteMissingConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		err := s.DeleteConfig(ctx, id, repair.UnitConfig, "id")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PutAndGetConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		config := validConfig()
		config.RetryLimit = nil
		config.RetryBackoffSeconds = nil

		if err := s.PutConfig(ctx, id, repair.UnitConfig, "id", config); err != nil {
			t.Fatal(err)
		}
		actual, err := s.GetConfig(ctx, id, repair.UnitConfig, "id")
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(actual, config); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("PutAndDeleteConfig", func(t *testing.T) {
		t.Parallel()
		id, _ := gocql.RandomUUID()

		config := validConfig()

		if err := s.PutConfig(ctx, id, repair.UnitConfig, "id", config); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteConfig(ctx, id, repair.UnitConfig, "id"); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetConfig(ctx, id, repair.UnitConfig, "id")
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
	parallelNodeLimit := 0
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
