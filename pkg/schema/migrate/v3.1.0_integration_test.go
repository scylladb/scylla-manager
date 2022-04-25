// Copyright (C) 2021 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"context"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestSetExistingTasksDeletedIntegration(t *testing.T) {
	type Task struct {
		ClusterID uuid.UUID
		Type      string
		ID        uuid.UUID
		Name      string
		Deleted   bool
	}

	clusterID := uuid.MustRandom()
	input := []*Task{
		{
			ClusterID: clusterID,
			Type:      "repair",
			ID:        uuid.MustParse("6D77D539-1D10-4A40-98F2-4DE0865DE183"),
		},
		{
			ClusterID: clusterID,
			Type:      "backup",
			ID:        uuid.MustParse("8042889C-6226-4FBA-BEAD-13C768FC2FC8"),
		},
	}

	prepare := func(session gocqlx.Session) error {
		q := qb.Insert("scheduler_task").Columns("cluster_id", "type", "id").Query(session)
		for _, t := range input {
			if err := q.BindStruct(t).Exec(); err != nil {
				return err
			}
		}
		q.Release()
		return nil
	}

	validate := func(session gocqlx.Session) error {
		q := qb.Select("scheduler_task").Where(qb.EqLit("deleted", "false")).AllowFiltering().Query(session)
		defer q.Release()

		var all []*Task
		if err := q.Iter().Unsafe().Select(&all); err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		for i, v := range all {
			Logger.Debug(ctx, "Tasks", "pos", i, "task", v)
		}

		if len(all) != 2 {
			t.Fatalf("Expected 2 not deleted, got %d", len(all))
		}

		return nil
	}

	testCallback(t, migrate.CallComment, "setExistingTasksDeleted", prepare, validate)
}
