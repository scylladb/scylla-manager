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

ALTER TABLE repair_run ADD end_time timestamp;

CREATE TYPE token_range (
    start_token bigint,
    end_token   bigint,
);

ALTER TABLE repair_run_state DROP success_pos;
ALTER TABLE repair_run_state DROP error_pos;
ALTER TABLE repair_run_state ADD success_ranges set<frozen<token_range>>;
