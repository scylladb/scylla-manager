-- Backup

CREATE TYPE backup_unit (
    keyspace_name text,
    tables set<text>,
);

CREATE TABLE backup_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    prev_id timeuuid,
    snapshot_tag text,
    units list<frozen<backup_unit>>,
    dc list<text>,
    location list<text>,
    start_time timestamp,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE backup_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    agent_job_id bigint,
    host text,
    unit int,
    table_name text,
    file_name text,
    started_at timestamp,
    completed_at timestamp,
    error text,
    size bigint,
    uploaded bigint,
    skipped bigint,
    failed bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), host, unit, table_name, file_name)
);
