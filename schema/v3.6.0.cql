CREATE TABLE one2onerestore_run_table_progress (
    cluster_id           uuid,
    task_id              uuid,
    run_id               uuid,

    started_at           timestamp,
    completed_at         timestamp,

    keyspace_name        text,
    table_name           text,

    host                 text,

    error                text,

    table_size           bigint,
    downloaded           bigint,
    versioned_downloaded bigint,
    is_refreshed         boolean,

    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name, host)
) WITH default_time_to_live = 15552000;

CREATE TABLE one2onerestore_run_view_progress (
    cluster_id        uuid,
    task_id           uuid,
    run_id            uuid,

    started_at        timestamp,
    completed_at      timestamp,

    keyspace_name     text,
    table_name        text,
    view_type         text,
    view_build_status text,

    error             text,

    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name, view_type)
) WITH default_time_to_live = 15552000;
