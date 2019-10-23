-- Backup

ALTER TABLE backup_run WITH default_time_to_live = 15552000;
ALTER TABLE backup_run_progress WITH default_time_to_live = 15552000;
ALTER TABLE scheduler_task WITH default_time_to_live = 15552000;
ALTER TABLE scheduler_task_run WITH default_time_to_live = 15552000;

-- As a consequence of updating TTL of scheduler_* tables we need to update repair too

ALTER TABLE repair_run WITH default_time_to_live = 15552000;
ALTER TABLE repair_run_progress WITH default_time_to_live = 15552000;