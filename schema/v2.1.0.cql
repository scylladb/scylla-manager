
-- Backup

ALTER TABLE backup_run_progress DROP files;

ALTER TABLE backup_run DROP done;

ALTER TABLE backup_run ADD stage text;

ALTER TABLE backup_run ADD nodes set<text>;
