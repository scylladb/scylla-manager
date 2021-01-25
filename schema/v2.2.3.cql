-- Drop job execution tracking because it's not efficient for large clusters.
DROP TABLE repair_job_execution;

-- Add time tracking fields to repair_run_progress to track start, end,
-- and duration progress for each host/keyspace/table combination.
ALTER TABLE repair_run_progress ADD started_at timestamp;
ALTER TABLE repair_run_progress ADD completed_at timestamp;
ALTER TABLE repair_run_progress ADD duration bigint;
ALTER TABLE repair_run_progress ADD duration_started_at timestamp;
