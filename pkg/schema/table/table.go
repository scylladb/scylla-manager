// Copyright (C) 2017 ScyllaDB

package table

import "github.com/scylladb/gocqlx/table"

// Table models
var (
	BackupRun = table.New(table.Metadata{
		Name: "backup_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"prev_id",
			"snapshot_tag",
			"units",
			"dc",
			"location",
			"start_time",
			"done",
		},
		PartKey: []string{"cluster_id", "task_id"},
		SortKey: []string{"id"},
	})

	BackupRunProgress = table.New(table.Metadata{
		Name: "backup_run_progress",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"agent_job_id",
			"host",
			"unit",
			"table_name",
			"started_at",
			"completed_at",
			"error",
			"size",
			"uploaded",
			"skipped",
			"failed",
		},
		PartKey: []string{"cluster_id", "task_id", "run_id"},
		SortKey: []string{"host", "unit", "table_name"},
	})

	Cluster = table.New(table.Metadata{
		Name: "cluster",
		Columns: []string{
			"id",
			"name",
			"known_hosts",
			"auth_token",
		},
		PartKey: []string{"id"},
	})

	RepairRun = table.New(table.Metadata{
		Name: "repair_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"prev_id",
			"topology_hash",
			"units",
			"dc",
			"host",
			"with_hosts",
			"token_ranges",
			"start_time",
		},
		PartKey: []string{"cluster_id", "task_id"},
		SortKey: []string{"id"},
	})

	RepairRunProgress = table.New(table.Metadata{
		Name: "repair_run_progress",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"unit",
			"host",
			"shard",
			"segment_count",
			"segment_success",
			"segment_error",
			"segment_error_start_tokens",
			"segment_error_pos",
			"last_start_token",
			"last_start_time",
			"last_command_id",
		},
		PartKey: []string{"cluster_id", "task_id", "run_id"},
		SortKey: []string{"unit", "host", "shard"},
	})

	SchedTask = table.New(table.Metadata{
		Name: "scheduler_task",
		Columns: []string{
			"cluster_id",
			"type",
			"id",
			"name",
			"tags",
			"enabled",
			"sched",
			"properties",
		},
		PartKey: []string{"cluster_id"},
		SortKey: []string{"type", "id"},
	})

	SchedRun = table.New(table.Metadata{
		Name: "scheduler_task_run",
		Columns: []string{
			"cluster_id",
			"type",
			"task_id",
			"id",
			"status",
			"cause",
			"owner",
			"start_time",
			"end_time",
		},
		PartKey: []string{"cluster_id", "type", "task_id"},
		SortKey: []string{"id"},
	})

	Secrets = table.New(table.Metadata{
		Name: "secrets",
		Columns: []string{
			"cluster_id",
			"key",
			"value",
		},
		PartKey: []string{"cluster_id"},
		SortKey: []string{"key"},
	})
)
