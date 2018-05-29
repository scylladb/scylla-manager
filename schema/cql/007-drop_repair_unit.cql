-- Repair

DROP TABLE repair_unit;

-- recreate / truncate repair_run and repair_run_progress we change semantics
-- of unit_id to task_id and data would be deleted anyway due to TTL.

DROP TABLE repair_run;

CREATE TABLE repair_run (
    cluster_id uuid,
    task_id uuid,
    id timeuuid,
    cause text,
    end_time timestamp,
    keyspace_name text,
    prev_id timeuuid,
    start_time timestamp,
    status text,
    tables set<text>,
    topology_hash uuid,
    PRIMARY KEY ((cluster_id, task_id), id)
) WITH default_time_to_live = 360000 AND CLUSTERING ORDER BY (id DESC);

TRUNCATE repair_run_progress;

ALTER TABLE repair_run_progress RENAME unit_id TO task_id;
