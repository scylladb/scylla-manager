-- name: GetRepairRun :many
SELECT * FROM repair_run
WHERE cluster_id = ? AND task_id = ? AND id = ?;

-- name: InsertRepairRun :exec
REPLACE INTO repair_run (cluster_id, task_id, id, dc, end_time, host, intensity, parallel, prev_id, start_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetRepairRunProgress :many
SELECT * FROM repair_run_progress
WHERE cluster_id = ? AND task_id = ? AND run_id = ?;

-- name: InsertRepairRunProgress :exec
REPLACE INTO repair_run_progress (cluster_id, task_id, run_id, host, keyspace_name, table_name, completed_at, duration, duration_started_at, error, size, started_at, success, token_ranges)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);


-- name: GetRepairRunState :many
SELECT * FROM repair_run_state
WHERE cluster_id = ? AND task_id = ? AND run_id = ?;

-- name: InsertRepairRunState :exec
REPLACE INTO repair_run_state (cluster_id, task_id, run_id, keyspace_name, table_name, success_ranges)
VALUES (?, ?, ?, ?, ?, ?);
