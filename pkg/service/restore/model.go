// Copyright (C) 2023 ScyllaDB

package restore

import (
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []Location `json:"location"`
	Keyspace        []string   `json:"keyspace,omitempty"`
	SnapshotTag     string     `json:"snapshot_tag"`
	BatchSize       int        `json:"batch_size,omitempty"`
	Parallel        int        `json:"parallel,omitempty"`
	Transfers       int        `json:"transfers"`
	RateLimit       []DCLimit  `json:"rate_limit,omitempty"`
	AllowCompaction bool       `json:"allow_compaction,omitempty"`
	UnpinAgentCPU   bool       `json:"unpin_agent_cpu"`
	RestoreSchema   bool       `json:"restore_schema,omitempty"`
	RestoreTables   bool       `json:"restore_tables,omitempty"`
	Continue        bool       `json:"continue"`

	// Cache for host with access to remote location
	locationHosts map[Location][]string `json:"-"`
}

const (
	maxBatchSize = 0
	// maxTransfers are experimentally defined to 2*node_shard_cnt.
	maxTransfers = 0
	maxRateLimit = 0
)

func defaultTarget() Target {
	return Target{
		BatchSize: 2,
		Parallel:  0,
		Transfers: maxTransfers,
		Continue:  true,
	}
}

// validateProperties makes a simple validation of params set by user.
// It does not perform validations that require access to the service.
func (t Target) validateProperties(dcMap map[string][]string) error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if _, err := SnapshotTagTime(t.SnapshotTag); err != nil {
		return err
	}
	if t.BatchSize < 0 {
		return errors.New("batch size param has to be greater or equal to zero")
	}
	if t.Parallel < 0 {
		return errors.New("parallel param has to be greater or equal to zero")
	}
	if t.Transfers != scyllaclient.TransfersFromConfig && t.Transfers != maxTransfers && t.Transfers < 1 {
		return errors.New("transfers param has to be equal to -1 (set transfers to the value from scylla-manager-agent.yaml config) " +
			"or 0 (set transfers for fastest download) or greater than zero")
	}
	if err := CheckDCs(t.RateLimit, dcMap); err != nil {
		return errors.Wrap(err, "invalid rate limit")
	}
	if t.RestoreSchema == t.RestoreTables {
		return errors.New("choose EXACTLY ONE restore type ('--restore-schema' or '--restore-tables' flag)")
	}
	if t.RestoreSchema && t.Keyspace != nil {
		return errors.New("restore schema always restores 'system_schema.*' tables only, no need to specify '--keyspace' flag")
	}
	return nil
}

func (t Target) sortLocations() {
	sort.SliceStable(t.Location, func(i, j int) bool {
		return t.Location[i].String() < t.Location[j].String()
	})
}

// Run tracks restore progress, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID      uuid.UUID
	SnapshotTag string
	Stage       Stage

	RepairTaskID uuid.UUID // task ID of the automated post-restore repair
	// Cache that's initialized once for entire task
	Units []Unit
	Views []View
}

// Unit represents restored keyspace and its tables with their size.
type Unit struct {
	Keyspace string  `json:"keyspace" db:"keyspace_name"`
	Size     int64   `json:"size"`
	Tables   []Table `json:"tables"`
}

func (u Unit) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Marshal(info, f.Interface())
}

func (u *Unit) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

func unitsContainTable(units []Unit, ks, tab string) bool {
	idx := slices.IndexFunc(units, func(u Unit) bool { return u.Keyspace == ks })
	if idx < 0 {
		return false
	}
	return slices.ContainsFunc(units[idx].Tables, func(t Table) bool { return t.Table == tab })
}

// Table represents restored table, its size and original tombstone_gc mode.
type Table struct {
	Table       string          `json:"table" db:"table_name"`
	TombstoneGC tombstoneGCMode `json:"tombstone_gc"`
	Size        int64           `json:"size"`
}

func (t Table) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Marshal(info, f.Interface())
}

func (t *Table) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// ViewType either Materialized View or Secondary Index.
type ViewType string

// ViewType enumeration.
const (
	MaterializedView ViewType = "MaterializedView"
	SecondaryIndex   ViewType = "SecondaryIndex"
)

// View represents statement used for recreating restored (dropped) views.
type View struct {
	Keyspace   string   `json:"keyspace" db:"keyspace_name"`
	View       string   `json:"view" db:"view_name"`
	Type       ViewType `json:"type" db:"view_type"`
	BaseTable  string   `json:"base_table"`
	CreateStmt string   `json:"create_stmt"`
}

func (t View) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Marshal(info, f.Interface())
}

func (t *View) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(t), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// RunProgress describes progress of restoring a single batch.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	// Different DB name because of historical reasons and because we can't drop/alter clustering column
	RemoteSSTableDir string   `db:"manifest_path"`
	Keyspace         string   `db:"keyspace_name"`
	Table            string   `db:"table_name"`
	SSTableID        []string `db:"sstable_id"`

	Host       string // IP of the node to which SSTables are downloaded.
	AgentJobID int64

	DownloadStartedAt   *time.Time
	DownloadCompletedAt *time.Time
	RestoreStartedAt    *time.Time
	RestoreCompletedAt  *time.Time
	Error               string
	Downloaded          int64
	Skipped             int64
	Failed              int64
	VersionedProgress   int64
}

// ForEachTableProgress iterates over all TableProgress belonging to the same run/manifest/table as the receiver.
func (pr *RunProgress) ForEachTableProgress(session gocqlx.Session, cb func(*RunProgress)) error {
	q := qb.Select(table.RestoreRunProgress.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
		qb.Eq("run_id"),
		qb.Eq("manifest_path"),
		qb.Eq("keyspace_name"),
		qb.Eq("table_name"),
	).Query(session)
	defer q.Release()

	iter := q.BindMap(qb.M{
		"cluster_id":    pr.ClusterID,
		"task_id":       pr.TaskID,
		"run_id":        pr.RunID,
		"manifest_path": pr.RemoteSSTableDir,
		"keyspace_name": pr.Keyspace,
		"table_name":    pr.Table,
	}).Iter()

	res := new(RunProgress)
	for iter.StructScan(res) {
		cb(res)
	}
	return iter.Close()
}

func (pr *RunProgress) setRestoreStartedAt() {
	t := timeutc.Now()
	pr.RestoreStartedAt = &t
}

func (pr *RunProgress) setRestoreCompletedAt() {
	t := timeutc.Now()
	pr.RestoreCompletedAt = &t
}

func validateTimeIsSet(t *time.Time) bool {
	return t != nil && !t.IsZero()
}

type progress struct {
	Size        int64      `json:"size"`
	Restored    int64      `json:"restored"`
	Downloaded  int64      `json:"downloaded"`
	Failed      int64      `json:"failed"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// Progress groups restore progress for all restored keyspaces.
type Progress struct {
	progress
	RepairProgress *repair.Progress `json:"repair_progress"`

	SnapshotTag string             `json:"snapshot_tag"`
	Keyspaces   []KeyspaceProgress `json:"keyspaces,omitempty"`
	Views       []ViewProgress     `json:"views,omitempty"`
	Stage       Stage              `json:"stage"`
}

// KeyspaceProgress groups restore progress for the tables belonging to this keyspace.
type KeyspaceProgress struct {
	progress

	Keyspace string          `json:"keyspace"`
	Tables   []TableProgress `json:"tables,omitempty"`
}

// TableProgress defines restore progress for the table.
type TableProgress struct {
	progress

	Table       string          `json:"table"`
	TombstoneGC tombstoneGCMode `json:"tombstone_gc"`
	Error       string          `json:"error,omitempty"`
}

// ViewProgress defines restore progress for the view.
type ViewProgress struct {
	View

	Status scyllaclient.ViewBuildStatus `json:"status"`
}

// TableName represents full table name.
type TableName struct {
	Keyspace string
	Table    string
}

func (t TableName) String() string {
	return t.Keyspace + "." + t.Table
}

// HostInfo represents host with rclone download config.
type HostInfo struct {
	Host      string
	Transfers int
	RateLimit int
}
