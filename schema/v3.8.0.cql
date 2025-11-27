CREATE TABLE tablet_repair_run_progress (
    cluster_id           uuid,
    task_id              uuid,
    run_id               uuid,

    keyspace_name        text,
    table_name           text,

    started_at           timestamp,
    completed_at         timestamp,
    error                text,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH default_time_to_live = 15552000;