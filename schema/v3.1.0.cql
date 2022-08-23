ALTER TABLE scheduler_task
    ADD deleted boolean;

-- CALL setExistingTasksDeleted;

CREATE TABLE restore_run (
    cluster_id    uuid,
    task_id       uuid,
    id            uuid,
    prev_id       uuid,
    manifest_path text,
    keyspace_name text,
    table_name    text,
    snapshot_tag  text,
    stage         text,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE restore_run_progress (
    cluster_id    uuid,
    task_id       uuid,
    run_id        uuid,
    manifest_path text,
    keyspace_name text,
    table_name    text,
    host          text,
    agent_job_id  bigint,
    sstable_id    list<text>,
    manifest_ip   text,
    started_at    timestamp,
    completed_at  timestamp,
    error         text,
    size          bigint,
    uploaded      bigint,
    skipped       bigint,
    failed        bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), manifest_path, keyspace_name, table_name, host, agent_job_id)
) WITH default_time_to_live = 15552000;