ALTER TYPE restore_view ADD build_status text;
ALTER TABLE restore_run_progress DROP skipped;
ALTER TABLE restore_run_progress ADD restored bigint;
