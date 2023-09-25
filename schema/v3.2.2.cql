ALTER TABLE repair_run_progress ADD size bigint;

ALTER TABLE repair_run ADD parallel int;
ALTER TABLE repair_run ADD intensity int;