-- Repair

CREATE TABLE IF NOT EXISTS repair_unit (
    cluster_id uuid,
    id uuid,
    name text,
    keyspace_name text,
    tables set<text>,
    PRIMARY KEY (cluster_id, id)
);

CREATE TABLE IF NOT EXISTS repair_config (
    cluster_id uuid,
    type text,
    external_id text,
    enabled boolean,
    segment_size_limit int,
    retry_limit int,
    retry_backoff_seconds int,
    parallel_shard_percent float,
    PRIMARY KEY (cluster_id, type, external_id)
);

INSERT INTO repair_config (cluster_id, type,external_id, enabled, segment_size_limit, retry_limit, retry_backoff_seconds, parallel_shard_percent)
VALUES (00000000-0000-4000-8000-000000000000, 'tenant', '', true, -1, 3, 60, 1);

CREATE TABLE IF NOT EXISTS repair_run (
    cluster_id uuid,
    unit_id uuid,
    id timeuuid,
    prev_id timeuuid,
    topology_hash uuid,
    keyspace_name text,
    tables set<text>,
    status text,
    cause text,
    restart_count int,
    start_time timestamp,
    end_time timestamp,
    PRIMARY KEY(cluster_id, unit_id, id)
) WITH default_time_to_live = 360000 AND CLUSTERING ORDER BY (unit_id ASC, id DESC);

CREATE TABLE IF NOT EXISTS repair_run_progress (
    cluster_id uuid,
    unit_id uuid,
    run_id uuid,
    host text,
    shard int,
    segment_count int,
    segment_success int,
    segment_error int,
    segment_error_start_tokens list<bigint>,
    last_start_token bigint,
    last_start_time timestamp,
    last_command_id int,
    PRIMARY KEY((cluster_id, unit_id, run_id), host, shard)
) WITH default_time_to_live = 360000;
