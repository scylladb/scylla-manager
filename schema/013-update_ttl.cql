-- Repair

ALTER TABLE repair_run WITH default_time_to_live = 2592000;
ALTER TABLE repair_run_progress WITH default_time_to_live = 2592000;

-- Scheduler

ALTER TABLE scheduler_task_run WITH default_time_to_live = 2592000;
