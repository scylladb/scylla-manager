ALTER TYPE restore_view ADD build_status text;

CREATE TABLE one2onerestore_run_progress (
    cluster_id         uuid,
    task_id            uuid,
    run_id             uuid,

    remote_sstable_dir text,
    keyspace_name      text,
    table_name         text,
    table_size         bigint,
    tombstone_gc       text,

    host               text,
    shard_cnt          bigint,

    agent_job_id       bigint,
    scylla_task_id     text,

    started_at         timestamp,
    completed_at       timestamp,

    error              text,
    downloaded         bigint,
    skipped            bigint,
    failed             bigint,
    versioned_progress bigint,

    view_name          text,
    view_type          text,
    view_build_status  text,

    stage              text,
    PRIMARY KEY ((cluster_id, task_id, run_id), remote_sstable_dir, keyspace_name, table_name, view_name, host, agent_job_id, scylla_task_id)
) WITH default_time_to_live = 15552000;
