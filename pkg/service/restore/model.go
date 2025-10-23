// Copyright (C) 2023 ScyllaDB

package restore

import (
	"encoding/json"
	"reflect"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []backupspec.Location `json:"location"`
	Keyspace        []string              `json:"keyspace,omitempty"`
	SnapshotTag     string                `json:"snapshot_tag"`
	BatchSize       int                   `json:"batch_size,omitempty"`
	Parallel        int                   `json:"parallel,omitempty"`
	Transfers       int                   `json:"transfers"`
	RateLimit       []backup.DCLimit      `json:"rate_limit,omitempty"`
	AllowCompaction bool                  `json:"allow_compaction,omitempty"`
	UnpinAgentCPU   bool                  `json:"unpin_agent_cpu"`
	RestoreSchema   bool                  `json:"restore_schema,omitempty"`
	RestoreTables   bool                  `json:"restore_tables,omitempty"`
	Continue        bool                  `json:"continue"`
	DCMappings      map[string]string     `json:"dc_mapping"`
	Method          Method                `json:"method,omitempty"`

	locationInfo []LocationInfo
}

// Method describes which API should be used by SM during restore.
type Method string

const (
	// MethodAuto means that SM will use native restore API when possible, and rclone API otherwise.
	MethodAuto Method = "auto"
	// MethodRclone means that SM will only use rclone API.
	MethodRclone Method = "rclone"
	// MethodNative means that SM will only use native scylla restore API (whether it's configured or not).
	MethodNative Method = "native"
)

// LocationInfo contains information about Location, such as what DCs it has,
// what hosts can access what dcs, and the list of manifests from this location.
type LocationInfo struct {
	// DC contains all data centers that can be found in this location
	DC []string
	// Contains hosts that should handle DCs from this location
	// after DCMappings are applied.
	DCHosts  map[string][]string
	Location backupspec.Location

	// Manifest in this Location. Shouldn't contain manifests from DCs
	// that are not in the DCMappings.
	Manifest []*backupspec.ManifestInfo
}

// AnyHost returns random host with access to this Location.
func (l LocationInfo) AnyHost() string {
	for _, hosts := range l.DCHosts {
		if len(hosts) != 0 {
			return hosts[0]
		}
	}
	return ""
}

// AllHosts returns all hosts with the access to this Location.
func (l LocationInfo) AllHosts() []string {
	hosts := strset.New()
	for _, h := range l.DCHosts {
		hosts.Add(h...)
	}
	return hosts.List()
}

const (
	maxBatchSize = 0
	// maxTransfers are experimentally defined to 2*node_shard_cnt.
	maxTransfers  = 0
	maxRateLimit  = 0
	defaultMethod = MethodRclone
)

func defaultTarget() Target {
	return Target{
		BatchSize: 2,
		Parallel:  0,
		Transfers: maxTransfers,
		Continue:  true,
		Method:    defaultMethod,
	}
}

// parseTarget parse Target from properties and applies defaults.
func parseTarget(properties json.RawMessage) (Target, error) {
	t := defaultTarget()
	if err := json.Unmarshal(properties, &t); err != nil {
		return Target{}, err
	}
	t.Method = MethodRclone
	return t, t.validateProperties()
}

// validateProperties makes a simple validation of params set by user.
// It does not perform validations that require access to the service.
func (t Target) validateProperties() error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if _, err := backupspec.SnapshotTagTime(t.SnapshotTag); err != nil {
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
	if t.RestoreSchema == t.RestoreTables {
		return errors.New("choose EXACTLY ONE restore type ('--restore-schema' or '--restore-tables' flag)")
	}
	if t.RestoreSchema && t.Keyspace != nil {
		return errors.New("restore schema always restores 'system_schema.*' tables only, no need to specify '--keyspace' flag")
	}
	if t.RestoreSchema && t.Method != defaultMethod {
		return errors.New("restore schema does not support '--method' flag")
	}
	// Check for duplicates in Location
	allLocations := strset.New()
	for _, l := range t.Location {
		p := l.RemotePath("")
		if allLocations.Has(p) {
			return errors.Errorf("location %s is specified multiple times", l)
		}
		allLocations.Add(p)
	}
	if !slices.Contains([]Method{MethodAuto, MethodRclone, MethodNative}, t.Method) {
		return errors.New("unknown method: " + string(t.Method))
	}
	return nil
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
	MaterializedView               ViewType = "MaterializedView"
	SecondaryIndex                 ViewType = "SecondaryIndex"
	AlternatorGlobalSecondaryIndex ViewType = "AlternatorGlobalSecondaryIndex"
	AlternatorLocalSecondaryIndex  ViewType = "AlternatorLocalSecondaryIndex"
)

// View represents statement used for recreating restored (dropped) views.
// It primarily uses CQL names, because those names are also used for
// interacting with views with scylla rest api.
type View struct {
	Keyspace  string   `json:"keyspace" db:"keyspace_name"` // CQL keyspace name. There is no ks abstraction in alternator.
	View      string   `json:"view" db:"view_name"`         // CQL view name. Different from alternator name.
	Type      ViewType `json:"type" db:"view_type"`
	BaseTable string   `json:"base_table"` // CQL name of the base table. Same as alternator name.
	// For cql views, CreateStmt is the text encoded cql statement.
	// For alternator GSIs, CreateStmt is the json encoded dynamodb.UpdateTableInput.
	// For alternator LSIs, CreateStmt is empty, as we don't drop and re-create them.
	CreateStmt  string                       `json:"create_stmt"`
	BuildStatus scyllaclient.ViewBuildStatus `json:"status"`
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

	RemoteSSTableDir string `db:"remote_sstable_dir"`
	Keyspace         string `db:"keyspace_name"`
	Table            string `db:"table_name"`
	Host             string // IP of the node which restores the SSTables.
	// Downloading SSTables could be done by either Rclone or Scylla API.
	// In case of Scylla API, it also streams the sstables into the cluster.
	AgentJobID   int64  `db:"agent_job_id"`
	ScyllaTaskID string `db:"scylla_task_id"`

	SSTableID          []string `db:"sstable_id"`
	RestoreStartedAt   *time.Time
	RestoreCompletedAt *time.Time
	// DownloadStartedAt and DownloadCompletedAt are within the
	// RestoreStartedAt and RestoreCompletedAt time frame.
	// They are set only when Rclone API is used, because
	// Scylla API downloads and restores SSTables as a single call.
	DownloadStartedAt   *time.Time
	DownloadCompletedAt *time.Time
	Restored            int64
	Downloaded          int64
	VersionedDownloaded int64
	Failed              int64
	Error               string
	ShardCnt            int64 // Host shard count used for bandwidth per shard calculation.
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
	Hosts       []HostProgress     `json:"hosts,omitempty"`
	Views       []View             `json:"views,omitempty"`
	Stage       Stage              `json:"stage"`
}

// KeyspaceProgress groups restore progress for the tables belonging to this keyspace.
type KeyspaceProgress struct {
	progress

	Keyspace string          `json:"keyspace"`
	Tables   []TableProgress `json:"tables,omitempty"`
}

// HostProgress groups restore progress for the host.
type HostProgress struct {
	Host             string `json:"host"`
	ShardCnt         int64  `json:"shard_cnt"`
	RestoredBytes    int64  `json:"restored_bytes"`
	RestoreDuration  int64  `json:"restore_duration"`
	DownloadedBytes  int64  `json:"downloaded_bytes"`
	DownloadDuration int64  `json:"download_duration"`
	StreamedBytes    int64  `json:"streamed_bytes"`
	StreamDuration   int64  `json:"stream_duration"`
}

// TableProgress defines restore progress for the table.
type TableProgress struct {
	progress

	Table       string          `json:"table"`
	TombstoneGC tombstoneGCMode `json:"tombstone_gc"`
	Error       string          `json:"error,omitempty"`
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
