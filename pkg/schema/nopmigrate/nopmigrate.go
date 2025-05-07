// Copyright (C) 2021 ScyllaDB

package nopmigrate

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/dbutil"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/table"
)

var reg = make(migrate.CallbackRegister)

func nopCallback(context.Context, gocqlx.Session, migrate.CallbackEvent, string) error {
	return nil
}

func init() {
	reg.Add(migrate.CallComment, "rewriteHealthCheck30", nopCallback)
	reg.Add(migrate.CallComment, "setExistingTasksDeleted", nopCallback)
	reg.Add(migrate.CallComment, "MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID", MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID)
}

// Callback is a sibling of migrate.Callback that registers all mandatory callbacks as nop.
var Callback = reg.Callback

// MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID needs to be implemented here
// and not in the migrate pkg, because of an import cycle.
func MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID(_ context.Context, session gocqlx.Session, _ migrate.CallbackEvent, _ string) error {
	const createDstStmt = `CREATE TABLE %s (
    cluster_id            uuid,
    task_id               uuid,
    run_id                uuid,
    remote_sstable_dir    text,
    keyspace_name         text,
    table_name            text,
    host                  text,
    agent_job_id          bigint,
    scylla_task_id        text,
    sstable_id            list<text>,
    restore_started_at    timestamp,
    restore_completed_at  timestamp,
    download_started_at   timestamp,
    download_completed_at timestamp,
    restored              bigint,
    downloaded            bigint,
    versioned_downloaded  bigint,
    failed                bigint,
    error                 text,
    shard_cnt             bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), remote_sstable_dir, keyspace_name, table_name, host, agent_job_id, scylla_task_id)
) WITH default_time_to_live = 15552000;`

	const dropTableStmt = `DROP TABLE %s;`

	src := table.New(table.Metadata{
		Name: "restore_run_progress",
		Columns: []string{
			"agent_job_id",
			"cluster_id",
			"download_completed_at",
			"download_started_at",
			"downloaded",
			"error",
			"failed",
			"host",
			"keyspace_name",
			"manifest_path",
			"restore_completed_at",
			"restore_started_at",
			"run_id",
			"shard_cnt",
			"sstable_id",
			"table_name",
			"task_id",
			"versioned_progress",
			"versioned_downloaded",
			"remote_sstable_dir",
			"scylla_task_id",
			"restored",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"manifest_path",
			"keyspace_name",
			"table_name",
			"host",
			"agent_job_id",
		},
	})

	dstMetadata := table.Metadata{
		Name: "restore_run_progress",
		Columns: []string{
			"agent_job_id",
			"cluster_id",
			"download_completed_at",
			"download_started_at",
			"downloaded",
			"error",
			"failed",
			"host",
			"keyspace_name",
			"remote_sstable_dir",
			"restore_completed_at",
			"restore_started_at",
			"restored",
			"run_id",
			"scylla_task_id",
			"shard_cnt",
			"sstable_id",
			"table_name",
			"task_id",
			"versioned_downloaded",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"remote_sstable_dir",
			"keyspace_name",
			"table_name",
			"host",
			"agent_job_id",
			"scylla_task_id",
		},
	}

	const (
		dstAndSrcName = "restore_run_progress"
		tmpName       = "tmp_restore_run_progress"
	)

	dst := table.New(dstMetadata)
	tmpMetadata := dstMetadata
	tmpMetadata.Name = tmpName
	tmp := table.New(tmpMetadata)

	if err := session.ExecStmt(fmt.Sprintf(createDstStmt, tmpName)); err != nil {
		return errors.Wrap(err, "create tmp_restore_run_progress")
	}

	err := dbutil.RewriteTable(session, tmp, src, func(m map[string]interface{}) {
		m["remote_sstable_dir"] = m["manifest_path"]
		delete(m, "manifest_path")

		m["versioned_downloaded"] = m["versioned_progress"]
		delete(m, "versioned_progress")
	})
	if err != nil {
		return errors.Wrap(err, "move contents of restore_run_progress to tmp_restore_run_progress")
	}

	if err := session.ExecStmt(fmt.Sprintf(dropTableStmt, dstAndSrcName)); err != nil {
		return errors.Wrap(err, "drop restore_run_progress")
	}
	if err := session.ExecStmt(fmt.Sprintf(createDstStmt, dstAndSrcName)); err != nil {
		return errors.Wrap(err, "recreate restore_run_progress")
	}

	if err := dbutil.RewriteTable(session, dst, tmp, nil); err != nil {
		return errors.Wrap(err, "move contents of tmp_restore_run_progress to restore_run_progress")
	}

	if err := session.ExecStmt(fmt.Sprintf(dropTableStmt, tmpName)); err != nil {
		return errors.Wrap(err, "drop tmp_restore_run_progress")
	}

	return nil
}
