// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"slices"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestMigrateToRestoreRunProgressWithSortKeyWithScyllaTaskIDIntegration(t *testing.T) {
	type old struct {
		ClusterID uuid.UUID
		TaskID    uuid.UUID
		RunID     uuid.UUID
		Keyspace  string `db:"keyspace_name"`
		Table     string `db:"table_name"`
		Host      string `db:"host"`

		ManifestPath      string `db:"manifest_path"`
		AgentJobID        int64  `db:"agent_job_id"`
		VersionedProgress int64  `db:"versioned_progress"`
	}

	type altered struct {
		ClusterID uuid.UUID
		TaskID    uuid.UUID
		RunID     uuid.UUID
		Keyspace  string `db:"keyspace_name"`
		Table     string `db:"table_name"`
		Host      string `db:"host"`

		RemoteSSTableDir    string `db:"remote_sstable_dir"`
		AgentJobID          int64  `db:"agent_job_id"`
		VersionedDownloaded int64  `db:"versioned_downloaded"`
	}

	id := uuid.MustRandom()
	input := []*old{
		{
			ClusterID:         id,
			TaskID:            id,
			RunID:             id,
			ManifestPath:      "a",
			AgentJobID:        1,
			VersionedProgress: 10,
		},
		{
			ClusterID:         id,
			TaskID:            id,
			RunID:             id,
			ManifestPath:      "b",
			AgentJobID:        2,
			VersionedProgress: 20,
		},
	}

	output := []*altered{
		{
			RemoteSSTableDir:    "a",
			AgentJobID:          1,
			VersionedDownloaded: 10,
		},
		{
			RemoteSSTableDir:    "b",
			AgentJobID:          2,
			VersionedDownloaded: 20,
		},
	}

	prepare := func(session gocqlx.Session) error {
		q := qb.Insert("restore_run_progress").Columns("cluster_id", "task_id", "run_id", "keyspace_name", "table_name", "host",
			"manifest_path", "agent_job_id", "versioned_progress").Query(session)
		for _, v := range input {
			if err := q.BindStruct(v).Exec(); err != nil {
				return err
			}
		}
		q.Release()
		return nil
	}

	validate := func(session gocqlx.Session) error {
		q := qb.Select("restore_run_progress").AllowFiltering().Query(session)
		defer q.Release()

		var all []*altered
		if err := q.Iter().Unsafe().Select(&all); err != nil {
			t.Fatal(err)
		}

		slices.SortFunc(all, func(a, b *altered) int { return int(a.AgentJobID - b.AgentJobID) })
		slices.SortFunc(output, func(a, b *altered) int { return int(a.AgentJobID - b.AgentJobID) })

		for i := range all {
			if all[i].RemoteSSTableDir != output[i].RemoteSSTableDir ||
				all[i].AgentJobID != output[i].AgentJobID ||
				all[i].VersionedDownloaded != output[i].VersionedDownloaded {
				t.Fatalf("got: %+#v\nexpected: %+#v", *all[i], *output[i])
			}
		}
		return nil
	}

	testCallback(t, migrate.CallComment, "MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID", prepare, validate)
}
