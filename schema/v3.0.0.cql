ALTER TYPE schedule ADD cron text;
ALTER TYPE schedule ADD window list<text>;
ALTER TYPE schedule ADD timezone text;
ALTER TYPE schedule ADD retry_wait bigint;

-- CALL rewriteHealthCheck30;

ALTER TABLE scheduler_task ADD status text;
ALTER TABLE scheduler_task ADD success_count int;
ALTER TABLE scheduler_task ADD error_count int;
ALTER TABLE scheduler_task ADD last_success timestamp;
ALTER TABLE scheduler_task ADD last_error timestamp;
ALTER TABLE scheduler_task ADD deleted boolean;

-- CALL setExistingTasksDeleted;

ALTER TABLE repair_run ADD host text;