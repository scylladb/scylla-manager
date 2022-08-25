// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// SnapshotInfo contains detailed information about snapshot.
type SnapshotInfo struct {
	SnapshotTag string `json:"snapshot_tag"`
	Nodes       int    `json:"nodes"`
	Size        int64  `json:"size"`
}

// ListItem represents contents of a snapshot within list boundaries.
type ListItem struct {
	ClusterID    uuid.UUID      `json:"cluster_id"`
	TaskID       uuid.UUID      `json:"task_id"`
	Units        []Unit         `json:"units"`
	SnapshotInfo []SnapshotInfo `json:"snapshot_info"`

	unitCache map[string]*strset.Set `json:"-"`
}

// Target specifies what should be backed up and where.
type Target struct {
	Units            []Unit       `json:"units,omitempty"`
	DC               []string     `json:"dc,omitempty"`
	Location         []Location   `json:"location"`
	Retention        int          `json:"retention"`
	RetentionDays    int          `json:"retention_days"`
	RetentionMap     RetentionMap `json:"-"` // policy for all tasks, injected in runtime
	RateLimit        []DCLimit    `json:"rate_limit,omitempty"`
	SnapshotParallel []DCLimit    `json:"snapshot_parallel,omitempty"`
	UploadParallel   []DCLimit    `json:"upload_parallel,omitempty"`
	Continue         bool         `json:"continue,omitempty"`
	PurgeOnly        bool         `json:"purge_only,omitempty"`

	// LiveNodes caches node status for GetTarget GetTargetSize calls.
	liveNodes scyllaclient.NodeStatusInfoSlice `json:"-"`
}

// Unit represents keyspace and its tables.
type Unit struct {
	Keyspace  string   `json:"keyspace" db:"keyspace_name"`
	Tables    []string `json:"tables,omitempty"`
	AllTables bool     `json:"all_tables"`
}

func (u Unit) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Marshal(info, f.Interface())
}

func (u *Unit) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

type Insertable interface {
	ExecInsertQuery(gocqlx.Session) error
}

type Deletable interface {
	ExecDeleteQuery(session gocqlx.Session) error
}

// stageNone is a special Stage indicating the there is no stage.
// This happens when working with runs coming from versions prior to adding stage.
const stageNone Stage = ""

// Run tracks backup progress, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID      uuid.UUID
	SnapshotTag string
	Units       []Unit
	DC          []string
	Nodes       []string
	Location    []Location
	StartTime   time.Time
	Stage       Stage
}

func (r *Run) ExecInsertQuery(s gocqlx.Session) error {
	q := table.BackupRun.InsertQuery(s).BindStruct(r)
	return q.ExecRelease()
}

type fileInfo struct {
	Name string
	Size int64
}

// RunProgress describes backup progress on per-file basis.
//
// Each RunProgress either has Uploaded or Skipped fields set to respective
// amount of bytes. Failed shows amount of bytes that is assumed to have
// failed. Since current implementation doesn't support resume at file level
// this value will always be the same as Uploaded as file needs to be uploaded
// again. In summary Failed is supposed to mean, out of uploaded bytes how many
// bytes have to be uploaded again.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	Host      string
	Unit      int64
	TableName string

	AgentJobID  int64
	StartedAt   *time.Time
	CompletedAt *time.Time
	Error       string
	Size        int64 // Total file size in bytes.
	Uploaded    int64 // Amount of total uploaded bytes.
	Skipped     int64 // Amount of skipped bytes because file was present.
	// Amount of bytes that have been uploaded but due to error have to be
	// uploaded again.
	Failed int64

	files []fileInfo
}

func (p *RunProgress) ExecInsertQuery(s gocqlx.Session) error {
	q := table.BackupRunProgress.InsertQuery(s).BindStruct(p)
	return q.ExecRelease()
}

// TotalUploaded returns total amount of uploaded bytes including skipped
// bytes.
func (p *RunProgress) TotalUploaded() int64 {
	return p.Uploaded + p.Skipped
}

// IsUploaded returns true if entire snapshot is uploaded.
func (p *RunProgress) IsUploaded() bool {
	return p.Size == p.TotalUploaded()
}

type progress struct {
	Size        int64      `json:"size"`
	Uploaded    int64      `json:"uploaded"`
	Skipped     int64      `json:"skipped"`
	Failed      int64      `json:"failed"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// Progress groups uploading progress for all backed up hosts.
type Progress struct {
	progress
	SnapshotTag string         `json:"snapshot_tag"`
	DC          []string       `json:"dcs,omitempty"`
	Hosts       []HostProgress `json:"hosts,omitempty"`
	Stage       Stage          `json:"stage"`
}

// HostProgress groups uploading progress for keyspaces belonging to this host.
type HostProgress struct {
	progress

	Host      string             `json:"host"`
	Keyspaces []KeyspaceProgress `json:"keyspaces,omitempty"`
}

// KeyspaceProgress groups uploading progress for the tables belonging to this
// keyspace.
type KeyspaceProgress struct {
	progress

	Keyspace string          `json:"keyspace"`
	Tables   []TableProgress `json:"tables,omitempty"`
}

// TableProgress defines progress for the table.
type TableProgress struct {
	progress

	Table string `json:"table"`
	Error string `json:"error,omitempty"`
}

// DCLimit specifies a rate limit for a DC.
type DCLimit struct {
	DC    string `json:"dc"`
	Limit int    `json:"limit"`
}

func (l DCLimit) String() string {
	p := fmt.Sprint(l.Limit)
	if l.DC != "" {
		p = l.DC + ":" + p
	}
	return p
}

func (l DCLimit) MarshalText() (text []byte, err error) {
	return []byte(l.String()), nil
}

func (l *DCLimit) UnmarshalText(text []byte) error {
	pattern := regexp.MustCompile(`^(([a-zA-Z0-9\-\_\.]+):)?([0-9]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid limit %q, the format is [dc:]<number>", string(text))
	}

	limit, err := strconv.ParseInt(string(m[3]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid limit value")
	}

	l.DC = string(m[2])
	l.Limit = int(limit)

	return nil
}

func dcLimitDCAtPos(s []DCLimit) func(int) (string, string) {
	return func(i int) (string, string) {
		return s[i].DC, s[i].String()
	}
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace         []string     `json:"keyspace"`
	DC               []string     `json:"dc"`
	Location         []Location   `json:"location"`
	Retention        int          `json:"retention"`
	RetentionDays    int          `json:"retention_days"`
	RetentionMap     RetentionMap `json:"retention_map"`
	RateLimit        []DCLimit    `json:"rate_limit"`
	SnapshotParallel []DCLimit    `json:"snapshot_parallel"`
	UploadParallel   []DCLimit    `json:"upload_parallel"`
	Continue         bool         `json:"continue"`
	PurgeOnly        bool         `json:"purge_only"`
}

func defaultTaskProperties() taskProperties {
	return taskProperties{
		Retention:     3,
		RetentionDays: 0,
		Continue:      true,
	}
}

func extractLocations(properties []json.RawMessage) ([]Location, error) {
	var (
		m         = strset.New()
		locations []Location
		errs      error
	)

	for i := range properties {
		var p taskProperties
		if err := json.Unmarshal(properties[i], &p); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "parse %q", string(properties[i])))
			continue
		}
		// Add location once
		for _, l := range p.Location {
			if key := l.RemotePath(""); !m.Has(key) {
				m.Add(key)
				locations = append(locations, l)
			}
		}
	}

	return locations, errs
}

// Retention defines the retention policy for a backup task.
type Retention struct {
	RetentionDays int
	Retention     int
}

// RetentionMap is a mapping of TaskIDs to retention policies.
type RetentionMap map[uuid.UUID]Retention

// ExtractRetention parses properties as task properties and returns "retention".
func ExtractRetention(properties json.RawMessage) (Retention, error) {
	var p taskProperties
	if err := json.Unmarshal(properties, &p); err != nil {
		return Retention{}, err
	}
	return Retention{p.RetentionDays, p.Retention}, nil
}
