-- Scheduler

CREATE TABLE scheduler_task (
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

CREATE TABLE scheduler_user_task (
    cluster_id uuid,
    type text,
    external_id text,
    PRIMARY KEY (cluster_id, type, external_id)
);

CREATE TABLE scheduler_task_run (
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

CREATE TABLE scheduler_active_run_by_cluster (
    cluster_id uuid,
    type text,
    external_id text,
    id timeuuid,
    PRIMARY KEY (cluster_id, type, external_id, id)
);
