-- Cluster

ALTER TABLE cluster DROP ssh_user;
ALTER TABLE cluster ADD auth_token text;

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
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

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
) WITH default_time_to_live = 15552000;

-- Scheduler & Repair adjust TTLs

ALTER TABLE scheduler_task WITH default_time_to_live = 15552000;
ALTER TABLE scheduler_task_run WITH default_time_to_live = 15552000;

ALTER TABLE repair_run WITH default_time_to_live = 15552000;
ALTER TABLE repair_run_progress WITH default_time_to_live = 15552000;

-- Secrets

CREATE TABLE secrets (
    cluster_id uuid,
    key text,
    value blob,
    PRIMARY KEY (cluster_id, key)
);