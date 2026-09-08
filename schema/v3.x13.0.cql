CREATE TABLE restore_run_progress_tablet (
    cluster_id           uuid,
    task_id              uuid,
    run_id               uuid,

    keyspace_name        text,
    table_name           text,

    host                 text,
    scylla_task_id       text,
    started_at           timestamp,
    completed_at         timestamp,
    restored_sstables    bigint,
    total_sstables       bigint,
    size                 bigint,
    error                text,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH default_time_to_live = 15552000;
