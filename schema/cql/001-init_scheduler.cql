-- Scheduler

CREATE TYPE schedule (
    start_date timestamp,
    repeat boolean,
    interval_days int,
    num_retries int
);

CREATE TABLE scheduler_task (
    cluster_id uuid,
    type text,
    external_id text,
    tags set<text>,
    metadata text,
    enabled boolean,
    sched frozen<schedule>,
    properties map<text, text>,
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
    PRIMARY KEY ((cluster_id, type, external_id), id)
) WITH CLUSTERING ORDER BY (id DESC);
