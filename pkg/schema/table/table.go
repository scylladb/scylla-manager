// Code generated by "gocqlx/cmd/schemagen"; DO NOT EDIT.

package table

import "github.com/scylladb/gocqlx/v2/table"

// Table models.
var (
	BackupRun = table.New(table.Metadata{
		Name: "backup_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"dc",
			"location",
			"nodes",
			"prev_id",
			"snapshot_tag",
			"stage",
			"start_time",
			"units",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
		},
		SortKey: []string{
			"id",
		},
	})

	BackupRunProgress = table.New(table.Metadata{
		Name: "backup_run_progress",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"host",
			"unit",
			"table_name",
			"agent_job_id",
			"completed_at",
			"error",
			"failed",
			"size",
			"skipped",
			"started_at",
			"uploaded",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"host",
			"unit",
			"table_name",
		},
	})

	Cluster = table.New(table.Metadata{
		Name: "cluster",
		Columns: []string{
			"id",
			"auth_token",
			"known_hosts",
			"name",
			"port",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{},
	})

	Drawer = table.New(table.Metadata{
		Name: "drawer",
		Columns: []string{
			"cluster_id",
			"key",
			"value",
		},
		PartKey: []string{
			"cluster_id",
		},
		SortKey: []string{
			"key",
		},
	})

	GocqlxMigrate = table.New(table.Metadata{
		Name: "gocqlx_migrate",
		Columns: []string{
			"name",
			"checksum",
			"done",
			"end_time",
			"start_time",
		},
		PartKey: []string{
			"name",
		},
		SortKey: []string{},
	})

	RepairRun = table.New(table.Metadata{
		Name: "repair_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"dc",
			"host",
			"prev_id",
			"start_time",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
		},
		SortKey: []string{
			"id",
		},
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
			"completed_at",
			"duration",
			"duration_started_at",
			"error",
			"started_at",
			"success",
			"token_ranges",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"host",
			"keyspace_name",
			"table_name",
		},
	})

	RepairRunState = table.New(table.Metadata{
		Name: "repair_run_state",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"keyspace_name",
			"table_name",
			"error_pos",
			"success_pos",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"keyspace_name",
			"table_name",
		},
	})

	RestoreRun = table.New(table.Metadata{
		Name: "restore_run",
		Columns: []string{
			"cluster_id",
			"task_id",
			"id",
			"keyspace_name",
			"location",
			"manifest_path",
			"prev_id",
			"repair_task_id",
			"snapshot_tag",
			"stage",
			"table_name",
			"units",
			"views",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
		},
		SortKey: []string{
			"id",
		},
	})

	RestoreRunProgress = table.New(table.Metadata{
		Name: "restore_run_progress",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"manifest_path",
			"keyspace_name",
			"table_name",
			"host",
			"agent_job_id",
			"download_completed_at",
			"download_started_at",
			"downloaded",
			"error",
			"failed",
			"restore_completed_at",
			"restore_started_at",
			"skipped",
			"sstable_id",
			"versioned_progress",
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

	SchedulerTask = table.New(table.Metadata{
		Name: "scheduler_task",
		Columns: []string{
			"cluster_id",
			"type",
			"id",
			"deleted",
			"enabled",
			"error_count",
			"last_error",
			"last_success",
			"name",
			"properties",
			"sched",
			"status",
			"success_count",
			"tags",
		},
		PartKey: []string{
			"cluster_id",
		},
		SortKey: []string{
			"type",
			"id",
		},
	})

	SchedulerTaskRun = table.New(table.Metadata{
		Name: "scheduler_task_run",
		Columns: []string{
			"cluster_id",
			"type",
			"task_id",
			"id",
			"cause",
			"end_time",
			"owner",
			"start_time",
			"status",
		},
		PartKey: []string{
			"cluster_id",
			"type",
			"task_id",
		},
		SortKey: []string{
			"id",
		},
	})

	Secrets = table.New(table.Metadata{
		Name: "secrets",
		Columns: []string{
			"cluster_id",
			"key",
			"value",
		},
		PartKey: []string{
			"cluster_id",
		},
		SortKey: []string{
			"key",
		},
	})

	ValidateBackupRunProgress = table.New(table.Metadata{
		Name: "validate_backup_run_progress",
		Columns: []string{
			"cluster_id",
			"task_id",
			"run_id",
			"dc",
			"host",
			"location",
			"broken_snapshots",
			"completed_at",
			"deleted_files",
			"manifests",
			"missing_files",
			"orphaned_bytes",
			"orphaned_files",
			"scanned_files",
			"started_at",
		},
		PartKey: []string{
			"cluster_id",
			"task_id",
			"run_id",
		},
		SortKey: []string{
			"dc",
			"host",
			"location",
		},
	})
)
