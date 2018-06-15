-- Scheduler

ALTER TABLE scheduler_task DROP properties;

ALTER TABLE scheduler_task ADD properties BLOB;
