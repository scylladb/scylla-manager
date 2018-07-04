-- Repair

DROP TABLE repair_unit;

-- recreate repair_run and repair_run_progress we change semantics
-- of unit_id to task_id and data would be deleted anyway due to TTL.

DROP TABLE repair_run;

CREATE TYPE IF NOT EXISTS repair_unit (
    keyspace_name text,
    tables set<text>,
    coordinator_dc text
);

CREATE TABLE repair_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    prev_id timeuuid,
    topology_hash uuid,
    units list<frozen<repair_unit>>,
    dc list<text>,
    token_ranges text,
    status text,
    cause text,
    start_time timestamp,
    end_time timestamp,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH default_time_to_live = 360000 AND CLUSTERING ORDER BY (id DESC);

DROP TABLE repair_run_progress;

CREATE TABLE repair_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    unit int,
    host text,
    shard int,
    segment_count int,
    segment_success int,
    segment_error int,
    segment_error_start_tokens list<bigint>,
    last_start_token bigint,
    last_start_time timestamp,
    last_command_id int,
    PRIMARY KEY((cluster_id, task_id, run_id), unit, host, shard)
) WITH default_time_to_live = 360000;
