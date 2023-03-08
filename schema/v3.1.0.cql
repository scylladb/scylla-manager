ALTER TABLE scheduler_task ADD deleted boolean;

-- CALL setExistingTasksDeleted;

-- Restore

CREATE TYPE restore_table (
    table_name text,
    size       bigint,
);

CREATE TYPE restore_unit (
    keyspace_name text,
    size          bigint,
    tables        set<frozen<restore_table>>,
);

CREATE TABLE restore_run (
    cluster_id    uuid,
    task_id       uuid,
    id            uuid,
    prev_id       uuid,
    location      text,
    manifest_path text,
    keyspace_name text,
    table_name    text,
    snapshot_tag  text,
    stage         text,
    units         list<frozen<restore_unit>>,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE restore_run_progress (
    cluster_id            uuid,
    task_id               uuid,
    run_id                uuid,
    manifest_path         text,
    keyspace_name         text,
    table_name            text,
    host                  text,
    agent_job_id          bigint,
    sstable_id            list<text>,
    download_started_at   timestamp,
    download_completed_at timestamp,
    restore_started_at    timestamp,
    restore_completed_at  timestamp,
    error                 text,
    downloaded            bigint,
    skipped               bigint,
    failed                bigint,
    versioned_progress    bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), manifest_path, keyspace_name, table_name, host, agent_job_id)
) WITH default_time_to_live = 15552000;