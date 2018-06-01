-- Repair

DROP TABLE repair_unit;

-- recreate / truncate repair_run and repair_run_progress we change semantics
-- of unit_id to task_id and data would be deleted anyway due to TTL.

DROP TABLE repair_run;

CREATE TYPE IF NOT EXISTS repair_unit (
    keyspace_name text,
    tables set<text>,
);

CREATE TABLE repair_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    prev_id timeuuid,
    topology_hash uuid,
    unit frozen<repair_unit>,
    status text,
    cause text,
    start_time timestamp,
    end_time timestamp,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH default_time_to_live = 360000 AND CLUSTERING ORDER BY (id DESC);

TRUNCATE repair_run_progress;

ALTER TABLE repair_run_progress RENAME unit_id TO task_id;
