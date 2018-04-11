-- Repair

ALTER TABLE repair_run_progress ADD segment_error_start_tokens list<bigint>;
