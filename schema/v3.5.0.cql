ALTER TYPE restore_view ADD build_status text;
ALTER TABLE backup_run_progress ADD scylla_task_id text;

DROP TABLE restore_run_progress;
CREATE TABLE restore_run_progress (
    cluster_id            uuid,
    task_id               uuid,
    run_id                uuid,
    remote_sstable_dir    text,
    keyspace_name         text,
    table_name            text,
    host                  text,
    agent_job_id          bigint,
    scylla_task_id        text,
    sstable_id            list<text>,
    restore_started_at    timestamp,
    restore_completed_at  timestamp,
    download_started_at   timestamp,
    download_completed_at timestamp,
    restored              bigint,
    downloaded            bigint,
    versioned_downloaded  bigint,
    failed                bigint,
    error                 text,
    shard_cnt             bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), remote_sstable_dir, keyspace_name, table_name, host, agent_job_id, scylla_task_id)
) WITH default_time_to_live = 15552000;