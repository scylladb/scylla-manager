-- TODO: change this file name to proper SM release
ALTER TABLE restore_run_progress ADD scylla_task_id text;
ALTER TABLE backup_run_progress ADD scylla_task_id text;