CREATE TABLE IF NOT EXISTS repair_run_progress (
    cluster_id text NOT NULL,
    task_id    text NOT NULL,
    run_id     text NOT NULL,

    host                text      NOT NULL,
    keyspace_name       text      NOT NULL,
    table_name          text      NOT NULL,
    completed_at        timestamp DEFAULT CURRENT_TIMESTAMP,
    duration            integer   NOT NULL DEFAULT 0,
    duration_started_at timestamp DEFAULT CURRENT_TIMESTAMP,
    error               integer   NOT NULL DEFAULT 0,
    size                integer   NOT NULL DEFAULT 0,
    started_at          timestamp DEFAULT CURRENT_TIMESTAMP,
    success             integer   NOT NULL DEFAULT 0,
    token_ranges        integer   NOT NULL DEFAULT 0,

    PRIMARY KEY (cluster_id, task_id, run_id, host, keyspace_name, table_name)
);

CREATE TABLE IF NOT EXISTS repair_run (
    cluster_id text NOT NULL,
    task_id    text NOT NULL,
    id         text NOT NULL,

    dc         blob      NOT     NULL DEFAULT '[]', -- array
    end_time   timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    host       text      NOT     NULL,
    intensity  int       DEFAULT 0,
    parallel   int       DEFAULT 0,
    prev_id    text      NOT     NULL DEFAULT '',
    start_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (cluster_id, task_id, id)
);

CREATE TABLE IF NOT EXISTS repair_run_state (
    cluster_id     text NOT NULL,
    task_id        text NOT NULL,
    run_id         text NOT NULL,

    keyspace_name  text NOT NULL,
    table_name     text NOT NULL,

    success_ranges blob NOT NULL DEFAULT '[]', -- array

    PRIMARY KEY (cluster_id, task_id, run_id, keyspace_name, table_name)
);
