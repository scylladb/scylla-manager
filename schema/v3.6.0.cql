ALTER TABLE backup_run_progress ADD scylla_task_id text;
ALTER TABLE restore_run_progress ADD scylla_task_id text;

ALTER TABLE restore_run_progress ADD versioned_downloaded bigint;
ALTER TABLE restore_run_progress ADD remote_sstable_dir text;

-- CALL MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID;