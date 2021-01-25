// Copyright (C) 2017 ScyllaDB

package table

import "github.com/scylladb/gocqlx/v2/table"

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
			"nodes",
			"location",
			"start_time",
			"stage",
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

	Drawer = table.New(table.Metadata{
		Name: "drawer",
		Columns: []string{
			"cluster_id",
			"key",
			"value",
		},
		PartKey: []string{"cluster_id"},
		SortKey: []string{"key"},
	})

	RepairRun = table.New(table.Metadata{
		Name: "repair_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"dc",
			"prev_id",
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
			"host",
			"keyspace_name",
			"table_name",
			"token_ranges",
			"success",
			"error",
			"started_at",
			"completed_at",
			"duration",
			"duration_started_at",
		},
		PartKey: []string{"cluster_id", "task_id", "run_id"},
		SortKey: []string{"host", "keyspace_name", "table_name"},
	})

	RepairRunState = table.New(table.Metadata{
		Name: "repair_run_state",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"keyspace_name",
			"table_name",
			"success_pos",
			"error_pos",
		},
		PartKey: []string{"cluster_id", "task_id", "run_id"},
		SortKey: []string{"keyspace_name", "table_name"},
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
