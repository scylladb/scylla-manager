-- DROP KEYSPACE IF EXISTS scylla_management;
-- CREATE KEYSPACE scylla_management WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- Scheduler

CREATE TABLE scylla_management.scheduler_task (
    external_id text,
    type text,
    cluster_id uuid,
    tags set<text>,
    metadata text,
    priority smallint,
    interval_days smallint,
    next_activation timestamp,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE scylla_management.scheduler_user_task (
    external_id text,
    type text,
    cluster_id uuid,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE scylla_management.scheduler_task_run (
    id timeuuid,
    external_id text,
    type text,
    cluster_id uuid,
    status text,
    cause text,
    owner text,
    start_time timestamp,
    end_time timestamp,
    pause_time timestamp,
    PRIMARY KEY (cluster_id, type, external_id, id)
);

CREATE TABLE scylla_management.scheduler_active_run_by_cluster (
    id timeuuid,
    external_id text,
    type text,
    cluster_id uuid,
    PRIMARY KEY (cluster_id, type, external_id, id)
);

-- Repair

CREATE TABLE scylla_management.repair_unit (
    id uuid,
    cluster_id uuid,
    keyspace_name text,
    tables set<text>,
    dc set<text>,
    PRIMARY KEY (cluster_id, id)
);

CREATE TABLE scylla_management.repair_config (
    external_id text,
    type text,
    cluster_id uuid,
    enabled boolean,
    timeout int,
    segment_count int,
    segment_retry int,
    segment_error_limit float,
    parallelism text,
    parallel_segment_limit int,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE scylla_management.repair_run (
    id timeuuid,
    unit_id uuid,
    cluster_id uuid,
    status text,
    cause text,
    start_time timestamp,
    end_time timestamp,
    pause_time timestamp,
    PRIMARY KEY(cluster_id, unit_id, id)
);

CREATE TABLE scylla_management.repair_run_segment (
    run_id uuid,
    unit_id uuid,
    cluster_id uuid,
    start_token bigint,
    end_token bigint,
    status text,
    cause text,
    coordinator_host text,
    command_id int,
    start_time timestamp,
    end_time timestamp,
    fail_count tinyint,
    PRIMARY KEY((cluster_id, unit_id, run_id), start_token)
);

-- Events

CREATE TABLE scylla_management.event (
    type text,
    external_id text,
    event_time timestamp,
    message text,
    PRIMARY KEY((external_id, type), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
