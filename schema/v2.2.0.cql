
-- Repair

DROP TABLE repair_run;
CREATE TABLE repair_run (
    cluster_id uuid,
    task_id uuid,
    id uuid,
    prev_id uuid,
    dc list<text>,
    start_time timestamp,
    PRIMARY KEY((cluster_id, task_id), id)
)  WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

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
    PRIMARY KEY ((cluster_id, task_id, run_id), host, keyspace_name, table_name)
) WITH default_time_to_live = 15552000;

CREATE TABLE repair_job_execution (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    host text,
    -- job_id is csv composite consisting of "node,id" because job is
    -- identified by the host it was executed on and it's integer id.
    -- This is workaround since tuples are not supported properly by gocql
    -- at the time of this writing (https://github.com/scylladb/gocql/issues/57).
    job_id text,
    start timestamp,
    end timestamp,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name, host, job_id)
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