-- Backup

CREATE TYPE backup_unit (
    keyspace_name text,
    tables set<text>,
    all_tables boolean
);

CREATE TABLE backup_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    prev_id timeuuid,
    units list<frozen<backup_unit>>,
    dc list<text>,
    location text,
    ttl timestamp,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE backup_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    host text,
    unit int,
    table_id int,
    size bigint,
    uploaded bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), host, unit, table_id)
);
