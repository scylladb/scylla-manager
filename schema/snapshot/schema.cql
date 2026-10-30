-- Schema produced by applying all migrations up to and including Version (see snapshot.go).
-- Used to initialize an empty keyspace in one step instead of replaying every migration.
-- Generated, do not edit: bump Version and run TestSnapshotMatchesMigrationsIntegration with -update.

CREATE TYPE IF NOT EXISTS backup_unit (
    keyspace_name text,
    tables set<text>
);

CREATE TYPE IF NOT EXISTS repair_unit (
    keyspace_name text,
    tables set<text>,
    coordinator_dc text
);

CREATE TYPE IF NOT EXISTS restore_table (
    table_name text,
    size bigint,
    tombstone_gc text
);

CREATE TYPE IF NOT EXISTS restore_unit (
    keyspace_name text,
    size bigint,
    tables set<frozen<restore_table>>
);

CREATE TYPE IF NOT EXISTS restore_view (
    keyspace_name text,
    view_name text,
    base_table text,
    view_type text,
    create_stmt text,
    build_status text
);

CREATE TYPE IF NOT EXISTS schedule (
    start_date timestamp,
    interval_seconds int,
    num_retries int,
    cron text,
    window list<text>,
    timezone text,
    retry_wait bigint
);

CREATE TYPE IF NOT EXISTS token_range (
    start_token bigint,
    end_token bigint
);

CREATE TABLE IF NOT EXISTS backup_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    dc list<text>,
    location list<text>,
    nodes set<text>,
    override_retention_lock boolean,
    prev_id timeuuid,
    retention_days int,
    retention_lock_mode text,
    snapshot_tag text,
    stage text,
    start_time timestamp,
    units list<frozen<backup_unit>>,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS backup_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    host text,
    unit int,
    table_name text,
    agent_job_id bigint,
    completed_at timestamp,
    error text,
    failed bigint,
    files_count bigint,
    files_skipped_count bigint,
    scylla_task_id text,
    size bigint,
    skipped bigint,
    started_at timestamp,
    uploaded bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), host, unit, table_name)
) WITH CLUSTERING ORDER BY (host ASC, unit ASC, table_name ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS cluster (
    id uuid,
    auth_token text,
    force_non_ssl_session_port boolean,
    force_tls_disabled boolean,
    host text,
    known_hosts set<text>,
    labels map<text, text>,
    name text,
    port int,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS drawer (
    cluster_id uuid,
    key text,
    value blob,
    PRIMARY KEY (cluster_id, key)
) WITH CLUSTERING ORDER BY (key ASC);

CREATE TABLE IF NOT EXISTS one2onerestore_run_table_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    host text,
    completed_at timestamp,
    downloaded bigint,
    error text,
    is_refreshed boolean,
    started_at timestamp,
    table_size bigint,
    versioned_downloaded bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name, host)
) WITH CLUSTERING ORDER BY (keyspace_name ASC, table_name ASC, host ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS one2onerestore_run_view_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    view_type text,
    completed_at timestamp,
    error text,
    started_at timestamp,
    view_build_status text,
    view_name text,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name, view_type)
) WITH CLUSTERING ORDER BY (keyspace_name ASC, table_name ASC, view_type ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS repair_run (
    cluster_id uuid,
    task_id uuid,
    id uuid,
    dc list<text>,
    end_time timestamp,
    host text,
    intensity int,
    parallel int,
    prev_id uuid,
    start_time timestamp,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS repair_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    host text,
    keyspace_name text,
    table_name text,
    completed_at timestamp,
    duration bigint,
    duration_started_at timestamp,
    error bigint,
    size bigint,
    started_at timestamp,
    success bigint,
    token_ranges bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), host, keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (host ASC, keyspace_name ASC, table_name ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS repair_run_state (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    success_ranges set<frozen<token_range>>,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (keyspace_name ASC, table_name ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS restore_run (
    cluster_id uuid,
    task_id uuid,
    id uuid,
    prev_id uuid,
    repair_task_id uuid,
    snapshot_tag text,
    stage text,
    units list<frozen<restore_unit>>,
    views list<frozen<restore_view>>,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS restore_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    remote_sstable_dir text,
    keyspace_name text,
    table_name text,
    host text,
    agent_job_id bigint,
    scylla_task_id text,
    download_completed_at timestamp,
    download_started_at timestamp,
    downloaded bigint,
    error text,
    failed bigint,
    restore_completed_at timestamp,
    restore_started_at timestamp,
    restored bigint,
    shard_cnt bigint,
    sstable_id list<text>,
    versioned_downloaded bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), remote_sstable_dir, keyspace_name, table_name, host, agent_job_id, scylla_task_id)
) WITH CLUSTERING ORDER BY (remote_sstable_dir ASC, keyspace_name ASC, table_name ASC, host ASC, agent_job_id ASC, scylla_task_id ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS restore_run_progress_tablet (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    completed_at timestamp,
    error text,
    host text,
    restored_sstables bigint,
    scylla_task_id text,
    size bigint,
    started_at timestamp,
    total_sstables bigint,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (keyspace_name ASC, table_name ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS scheduler_task (
    cluster_id uuid,
    type text,
    id uuid,
    deleted boolean,
    enabled boolean,
    error_count int,
    labels map<text, text>,
    last_error timestamp,
    last_success timestamp,
    name text,
    properties blob,
    sched frozen<schedule>,
    status text,
    success_count int,
    PRIMARY KEY (cluster_id, type, id)
) WITH CLUSTERING ORDER BY (type ASC, id ASC);

CREATE TABLE IF NOT EXISTS scheduler_task_run (
    cluster_id uuid,
    type text,
    task_id uuid,
    id timeuuid,
    cause text,
    end_time timestamp,
    owner text,
    start_time timestamp,
    status text,
    PRIMARY KEY ((cluster_id, type, task_id), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS secrets (
    cluster_id uuid,
    key text,
    value blob,
    PRIMARY KEY (cluster_id, key)
) WITH CLUSTERING ORDER BY (key ASC);

CREATE TABLE IF NOT EXISTS tablet_repair_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    keyspace_name text,
    table_name text,
    completed_at timestamp,
    error text,
    started_at timestamp,
    PRIMARY KEY ((cluster_id, task_id, run_id), keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (keyspace_name ASC, table_name ASC) AND default_time_to_live = 15552000;

CREATE TABLE IF NOT EXISTS validate_backup_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    dc text,
    host text,
    location text,
    broken_snapshots list<text>,
    completed_at timestamp,
    deleted_files int,
    manifests int,
    missing_files int,
    orphaned_bytes bigint,
    orphaned_files int,
    scanned_files int,
    started_at timestamp,
    PRIMARY KEY ((cluster_id, task_id, run_id), dc, host, location)
) WITH CLUSTERING ORDER BY (dc ASC, host ASC, location ASC);
