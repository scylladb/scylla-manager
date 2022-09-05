// Copyright (C) 2022 ScyllaDB

package backup

import (
	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

// RestoreTarget specifies what data should be restored and from which locations.
type RestoreTarget struct {
	Location      []Location `json:"location"`
	Keyspace      []string   `json:"keyspace,omitempty"`
	SnapshotTag   string     `json:"snapshot_tag"`
	BatchSize     int        `json:"batch_size,omitempty"`
	Parallel      int        `json:"parallel,omitempty"`
	RestoreSchema bool       `json:"restore_schema,omitempty"`
	RestoreTables bool       `json:"restore_tables,omitempty"`
	Continue      bool       `json:"continue,omitempty"`
}

func defaultRestoreTarget() RestoreTarget {
	return RestoreTarget{
		BatchSize: 2,
		Parallel:  1,
		Continue:  true,
	}
}

// validateProperties makes a simple validation of params set by user.
// It does not perform validations that require access to the service.
func (t RestoreTarget) validateProperties() error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if _, err := SnapshotTagTime(t.SnapshotTag); err != nil {
		return err
	}
	if t.BatchSize <= 0 {
		return errors.New("batch size param has to be greater than zero")
	}
	if t.Parallel <= 0 {
		return errors.New("parallel param has to be greater than zero")
	}
	if t.RestoreSchema == t.RestoreTables {
		return errors.New("choose EXACTLY ONE restore type ('--restore-schema' or '--restore-tables' flag)")
	}
	if t.RestoreSchema && t.Keyspace != nil {
		return errors.New("restore schema always restores 'system_schema.*' tables only, no need to specify '--keyspace' flag")
	}
	if t.RestoreSchema && t.Parallel > 1 {
		return errors.New("restore schema does not work in parallel, no need to specify '--parallel' flag")
	}
	return nil
}
