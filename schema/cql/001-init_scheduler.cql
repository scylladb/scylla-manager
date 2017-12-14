-- Scheduler

CREATE TYPE IF NOT EXISTS schedule (
    start_date timestamp,
    interval_days int,
    num_retries int
);

CREATE TABLE IF NOT EXISTS scheduler_task (
    cluster_id uuid,
    type text,
    id uuid,
    name text,
    tags set<text>,
    metadata text,
    enabled boolean,
    sched frozen<schedule>,
    properties map<text, text>,
    PRIMARY KEY (cluster_id, type, id)
);

CREATE TABLE IF NOT EXISTS scheduler_task_run (
    cluster_id uuid,
    type text,
    task_id uuid,
    id timeuuid,
    status text,
    cause text,
    owner text,
    start_time timestamp,
    end_time timestamp,
    PRIMARY KEY ((cluster_id, type, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC);
