ALTER TABLE restore_run ADD repair_task_id uuid;
ALTER TYPE restore_table ADD tombstone_gc text;