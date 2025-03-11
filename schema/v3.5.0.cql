ALTER TYPE restore_view ADD build_status text;
ALTER TABLE backup_run_progress ADD scylla_task_id text;

-- not used during restore
ALTER TABLE restore_run_progress DROP skipped;
-- replacing versioned_progress
ALTER TABLE restore_run_progress ADD versioned_downloaded bigint;
-- replacing manifest_path
ALTER TABLE restore_run_progress ADD remote_sstable_dir text;
-- important for native restore API
ALTER TABLE restore_run_progress ADD scylla_task_id text;
-- important for native restore API progress
ALTER TABLE restore_run_progress ADD restored bigint;

-- CALL MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID;