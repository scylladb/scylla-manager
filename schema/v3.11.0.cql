ALTER TABLE backup_run ADD retention_lock_mode text;
ALTER TABLE backup_run ADD override_retention_lock boolean;
ALTER TABLE backup_run ADD retention_days int;
