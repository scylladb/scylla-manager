-- Backup

CREATE TABLE validate_backup_run_progress (
    cluster_id uuid,
    task_id uuid,
    run_id uuid,
    dc text,
    host text,
    location text,
    manifests int,
    scanned_files int,
    broken_snapshots list<text>,
    missing_files int,
    orphaned_files int,
    orphaned_bytes bigint,
    deleted_files int,
    started_at timestamp,
    completed_at timestamp,
    PRIMARY KEY ((cluster_id, task_id, run_id), dc, host, location)
);

ALTER TABLE cluster ADD port int;
