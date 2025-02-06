// Copyright (C) 2017 ScyllaDB

package backup

import "github.com/scylladb/scylla-manager/backupspec"

type LocationValue backupspec.Location

var nilLocation = backupspec.Location{}

func (v *LocationValue) String() string {
	if v.Value() == nilLocation {
		return ""
	}
	return backupspec.Location(*v).String()
}

func (v *LocationValue) Set(s string) error {
	return (*backupspec.Location)(v).UnmarshalText([]byte(s))
}

func (v *LocationValue) Type() string {
	return "string"
}

func (v *LocationValue) Value() backupspec.Location {
	return backupspec.Location(*v)
}

type SnapshotTagValue string

func (v *SnapshotTagValue) String() string {
	return string(*v)
}

func (v *SnapshotTagValue) Set(s string) error {
	if !backupspec.IsSnapshotTag(s) {
		return backupspec.ErrInvalidSnapshotTag
	}
	*v = SnapshotTagValue(s)
	return nil
}

func (v *SnapshotTagValue) Type() string {
	return "string"
}

func (v *SnapshotTagValue) Value() string {
	return string(*v)
}
