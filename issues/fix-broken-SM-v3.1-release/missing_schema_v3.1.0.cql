USE scylla_manager;
ALTER TABLE restore_run_progress ADD versioned_progress bigint;
ALTER TABLE restore_run ADD location text;
UPDATE gocqlx_migrate SET checksum = '7cb98d445928959e06fcb9f893f10ad4' WHERE name = 'v3.1.0.cql';