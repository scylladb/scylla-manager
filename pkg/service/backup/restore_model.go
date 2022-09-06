// Copyright (C) 2022 ScyllaDB

package backup

import (
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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

// RestoreRun tracks restore progress, shares ID with scheduler.Run that initiated it.
type RestoreRun struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID       uuid.UUID
	ManifestPath string // marks currently processed manifest
	Keyspace     string `db:"keyspace_name"` // marks currently processed keyspace
	Table        string `db:"table_name"`    // marks currently processed table
	SnapshotTag  string
	Stage        RestoreStage

	Units []RestoreUnit // cache that's initialized once for entire task
}

// RestoreUnit represents restored keyspace and its tables with their size.
type RestoreUnit struct {
	Keyspace string `db:"keyspace_name"`
	Size     int64
	Tables   []RestoreTable
}

func (u RestoreUnit) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Marshal(info, f.Interface())
}

func (u *RestoreUnit) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// RestoreTable represents restored table and its size.
type RestoreTable struct {
	Table string `db:"table_name"`
	Size  int64
}

func (t RestoreTable) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Marshal(info, f.Interface())
}

func (t *RestoreTable) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// RestoreRunProgress describes restore progress (like in RunProgress) of
// already started download of SSTables with specified IDs to host.
type RestoreRunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	ManifestPath string
	Keyspace     string `db:"keyspace_name"`
	Table        string `db:"table_name"`
	Host         string // IP of the node to which SSTables are downloaded.
	AgentJobID   int64

	SSTableID           []string `db:"sstable_id"`
	DownloadStartedAt   *time.Time
	DownloadCompletedAt *time.Time
	RestoreStartedAt    *time.Time
	RestoreCompletedAt  *time.Time
	Error               string
	Downloaded          int64
	Skipped             int64
	Failed              int64
}

func validateTimeIsSet(t *time.Time) bool {
	return t != nil && !t.IsZero()
}

func (rp *RestoreRunProgress) setRestoreStartedAt() {
	t := timeutc.Now()
	rp.RestoreStartedAt = &t
}

func (rp *RestoreRunProgress) setRestoreCompletedAt() {
	t := timeutc.Now()
	rp.RestoreCompletedAt = &t
}

type restoreProgress struct {
	Size        int64      `json:"size"`
	Restored    int64      `json:"restored"`
	Downloaded  int64      `json:"downloaded"`
	Skipped     int64      `json:"skipped"`
	Failed      int64      `json:"failed"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// RestoreProgress groups restore progress for all restored keyspaces.
type RestoreProgress struct {
	restoreProgress

	SnapshotTag string                    `json:"snapshot_tag"`
	Keyspaces   []RestoreKeyspaceProgress `json:"keyspaces,omitempty"`
	Stage       RestoreStage              `json:"stage"`
}

// RestoreKeyspaceProgress groups restore progress for the tables belonging to this keyspace.
type RestoreKeyspaceProgress struct {
	restoreProgress

	Keyspace string                 `json:"keyspace"`
	Tables   []RestoreTableProgress `json:"tables,omitempty"`
}

// RestoreTableProgress defines restore progress for the table.
type RestoreTableProgress struct {
	restoreProgress

	Table string `json:"table"`
	Error string `json:"error,omitempty"`
}
