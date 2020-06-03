
-- Repair

ALTER TABLE repair_run DROP topology_hash;
ALTER TABLE repair_run DROP units;
ALTER TABLE repair_run DROP host;
ALTER TABLE repair_run DROP with_hosts;
ALTER TABLE repair_run DROP token_ranges;

DROP TABLE repair_run_progress;
CREATE TABLE repair_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    host text,
    keyspace_name text,
    table_name text,
    token_ranges bigint,
    success bigint,
    error bigint,
    started_at timestamp,
    completed_at timestamp,
    PRIMARY KEY ((cluster_id, task_id, run_id), host, keyspace_name, table_name)
) WITH default_time_to_live = 15552000;

CREATE TABLE repair_run_state (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    success_pos set<bigint>,
    error_pos set<bigint>,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH default_time_to_live = 15552000;