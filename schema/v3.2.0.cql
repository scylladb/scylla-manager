ALTER TABLE restore_run ADD repair_task_id uuid;
ALTER TYPE restore_table ADD tombstone_gc text;

CREATE TYPE restore_view (
    keyspace_name text,
    view_name     text,
    base_table    text,
    view_type     text,
    create_stmt   text,
);

ALTER TABLE restore_run ADD views list<frozen<restore_view>>;