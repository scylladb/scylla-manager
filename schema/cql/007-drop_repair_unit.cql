-- Repair

DROP TABLE repair_unit;

-- Truncate repair_run and repair_run_progress we change semantics of unit_id
-- to task_id and data would be deleted anyway due to TTL.

TRUNCATE repair_run;

ALTER TABLE repair_run RENAME unit_id TO task_id;

TRUNCATE repair_run_progress;

ALTER TABLE repair_run_progress RENAME unit_id TO task_id;

-- Drop not used restart_count

ALTER TABLE repair_run DROP restart_count ;
