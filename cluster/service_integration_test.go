// Copyright (C) 2017 ScyllaDB

// +build all integration

package cluster_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	s, err := cluster.NewService(session, nil, golog.NewDevelopment().Named("cluster"))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	cleanup := func(t *testing.T) {
		q := session.Query("TRUNCATE cluster")
		defer q.Release()
		if err := q.Exec(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("list empty clusters", func(t *testing.T) {
		cleanup(t)

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(clusters) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list clusters", func(t *testing.T) {
		cleanup(t)

		expected := make([]*cluster.Cluster, 3)
		for i := range expected {
			c := &cluster.Cluster{
				ID:         uuid.NewTime(),
				Name:       "name" + strconv.Itoa(i),
				Hosts:      []string{"a"},
				ShardCount: 16,
			}
			if err := s.PutCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			expected[i] = c
		}

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(clusters, expected, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing cluster", func(t *testing.T) {
		cleanup(t)

		c, err := s.GetClusterByID(ctx, uuid.MustRandom())
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("get cluster", func(t *testing.T) {
		cleanup(t)

		c0 := validCluster()
		c0.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}
		if c0.ID == uuid.Nil {
			t.Fatal("ID not updated")
		}
		c1, err := s.GetClusterByID(ctx, c0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}

		c2, err := s.GetClusterByName(ctx, c0.Name)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c2, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("put nil cluster", func(t *testing.T) {
		cleanup(t)

		if err := s.PutCluster(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put conflicting cluster", func(t *testing.T) {
		cleanup(t)

		c0 := validCluster()

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}

		c1 := c0
		c1.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put new cluster", func(t *testing.T) {
		cleanup(t)

		c := validCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if c.ID == uuid.Nil {
			t.Fatal("id not set")
		}
	})

	t.Run("delete missing cluster", func(t *testing.T) {
		cleanup(t)

		err := s.DeleteCluster(ctx, uuid.MustRandom())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete cluster", func(t *testing.T) {
		cleanup(t)

		c := validCluster()

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCluster(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetClusterByID(ctx, c.ID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})
}

func validCluster() *cluster.Cluster {
	return &cluster.Cluster{
		ID:         uuid.MustRandom(),
		Name:       "name_" + uuid.MustRandom().String(),
		Hosts:      []string{"a", "b"},
		ShardCount: 16,
	}
}
