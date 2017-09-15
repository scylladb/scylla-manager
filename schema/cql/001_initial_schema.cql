-- Scheduler

CREATE TABLE IF NOT EXISTS scylla_management.scheduler_task (
    cluster_id uuid,
    type text,
    external_id text,
    tags set<text>,
    metadata text,
    priority smallint,
    interval_days smallint,
    next_activation timestamp,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE IF NOT EXISTS scylla_management.scheduler_user_task (
    cluster_id uuid,
    type text,
    external_id text,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE IF NOT EXISTS scylla_management.scheduler_task_run (
    cluster_id uuid,
    type text,
    external_id text,
    id timeuuid,
    status text,
    cause text,
    owner text,
    start_time timestamp,
    end_time timestamp,
    pause_time timestamp,
    PRIMARY KEY (cluster_id, type, external_id, id)
);

CREATE TABLE IF NOT EXISTS scylla_management.scheduler_active_run_by_cluster (
    cluster_id uuid,
    type text,
    external_id text,
    id timeuuid,
    PRIMARY KEY (cluster_id, type, external_id, id)
);

-- Repair

CREATE TABLE IF NOT EXISTS scylla_management.repair_unit (
    cluster_id uuid,
    id uuid,
    keyspace_name text,
    tables set<text>,
    PRIMARY KEY (cluster_id, id)
);

CREATE TABLE IF NOT EXISTS scylla_management.repair_config (
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

INSERT INTO scylla_management.repair_config (cluster_id, type,external_id, enabled, segment_size_limit, retry_limit, retry_backoff_seconds, parallel_shard_percent)
VALUES (00000000-0000-4000-8000-000000000000, 'tenant', '', true, -1, 3, 60, 1);

CREATE TABLE IF NOT EXISTS scylla_management.repair_run (
    cluster_id uuid,
    unit_id uuid,
    id timeuuid,
    topology_hash uuid,
    keyspace_name text,
    tables set<text>,
    status text,
    cause text,
    restart_count int,
    start_time timestamp,
    end_time timestamp,
    pause_time timestamp,
    PRIMARY KEY(cluster_id, unit_id, id)
);

CREATE TABLE IF NOT EXISTS scylla_management.repair_run_progress (
    cluster_id uuid,
    unit_id uuid,
    run_id uuid,
    host text,
    shard int,
    segment_count int,
    segment_success int,
    segment_error int,
    last_start_token bigint,
    last_start_time timestamp,
    last_command_id int,
    PRIMARY KEY((cluster_id, unit_id, run_id), host, shard)
);

CREATE TABLE IF NOT EXISTS scylla_management.repair_run_error (
    cluster_id uuid,
    unit_id uuid,
    run_id uuid,
    start_token bigint,
    end_token bigint,
    status text,
    cause text,
    coordinator_host text,
    shard int,
    command_id int,
    start_time timestamp,
    end_time timestamp,
    fail_count int,
    PRIMARY KEY((cluster_id, unit_id, run_id), coordinator_host, shard, start_token)
);

-- Events

CREATE TABLE IF NOT EXISTS scylla_management.event (
    external_id text,
    type text,
    event_time timestamp,
    message text,
    PRIMARY KEY((external_id, type), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
