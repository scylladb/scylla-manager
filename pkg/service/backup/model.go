// Copyright (C) 2017 ScyllaDB

package backup

import (
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ListFilter specifies filtering for backup listing.
type ListFilter struct {
	ClusterID   uuid.UUID `json:"cluster_id"`
	DC          string    `json:"dc"`
	NodeID      string    `json:"node_id"`
	TaskID      uuid.UUID `json:"task_id"`
	Keyspace    []string  `json:"keyspace"`
	SnapshotTag string    `json:"snapshot_tag"`
	MinDate     time.Time `json:"min_date"`
	MaxDate     time.Time `json:"max_date"`
	Temporary   bool      `json:"temporary"`
}

// SnapshotInfo contains detailed information about snapshot.
type SnapshotInfo struct {
	SnapshotTag string `json:"snapshot_tag"`
	Size        int64  `json:"size"`
}

// SnapshotInfoSlice slice of SnapshotInfo.
type SnapshotInfoSlice []SnapshotInfo

// ListItem represents contents of a snapshot within list boundaries.
type ListItem struct {
	ClusterID    uuid.UUID         `json:"cluster_id"`
	Units        []Unit            `json:"units,omitempty"`
	SnapshotInfo SnapshotInfoSlice `json:"snapshot_info"`

	unitsHash uint64 // Internal usage only
}

func (d SnapshotInfoSlice) hasSnapshot(snapshotTag string) bool {
	for _, sd := range d {
		if sd.SnapshotTag == snapshotTag {
			return true
		}
	}
	return false
}

// FilesInfo specifies paths to files backed up for a table (and node) within
// a location.
// Note that a backup for a table usually consists of multiple instances of
// FilesInfo since data is replicated across many nodes.
type FilesInfo struct {
	Location Location    `json:"location"`
	Schema   string      `json:"schema"`
	Files    []filesInfo `json:"files"`
}

// filesInfo contains information about SST files of particular keyspace/table.
type filesInfo struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	Version  string   `json:"version"`
	Files    []string `json:"files"`
	Size     int64    `json:"size"`

	Path string `json:"path,omitempty"`
}

func makeFilesInfo(m *remoteManifest, filter *ksfilter.Filter) FilesInfo {
	fi := FilesInfo{
		Location: m.Location,
		Schema:   m.Content.Schema,
	}

	for _, idx := range m.Content.Index {
		if !filter.Check(idx.Keyspace, idx.Table) {
			continue
		}
		idx.Path = m.RemoteSSTableVersionDir(idx.Keyspace, idx.Table, idx.Version)
		fi.Files = append(fi.Files, idx)
	}

	return fi
}

// Target specifies what should be backed up and where.
type Target struct {
	Units            []Unit     `json:"units,omitempty"`
	DC               []string   `json:"dc,omitempty"`
	Location         []Location `json:"location"`
	Retention        int        `json:"retention"`
	RateLimit        []DCLimit  `json:"rate_limit"`
	SnapshotParallel []DCLimit  `json:"snapshot_parallel"`
	UploadParallel   []DCLimit  `json:"upload_parallel"`
	Continue         bool       `json:"continue"`
	// liveNodes caches node status for GetTarget GetTargetSize calls.
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

// Stage specifies the backup worker stage.
type Stage string

// Stage enumeration.
const (
	StageInit         Stage = "INIT"
	StageAwaitSchema  Stage = "AWAIT_SCHEMA"
	StageSnapshot     Stage = "SNAPSHOT"
	StageIndex        Stage = "INDEX"
	StageManifest     Stage = "MANIFEST"
	StageSchema       Stage = "SCHEMA"
	StageUpload       Stage = "UPLOAD"
	StageMoveManifest Stage = "MOVE_MANIFEST"
	StageMigrate      Stage = "MIGRATE"
	StagePurge        Stage = "PURGE"
	StageDone         Stage = "DONE"

	stageNone Stage = ""
)

var stageOrder = []Stage{
	StageInit,
	StageAwaitSchema,
	StageSnapshot,
	StageIndex,
	StageManifest,
	StageSchema,
	StageUpload,
	StageMoveManifest,
	StageMigrate,
	StagePurge,
	StageDone,
}

// Resumable run can be continued.
func (s Stage) Resumable() bool {
	switch s {
	case StageIndex, StageManifest, StageUpload, StageMoveManifest, StageMigrate, StagePurge:
		return true
	default:
		return false
	}
}

// Index returns stage position among all stages, stage with index n+1 happens
// after stage n.
func (s Stage) Index() int {
	for i := 0; i < len(stageOrder); i++ {
		if s == stageOrder[i] {
			return i
		}
	}
	panic("Unknown stage " + s)
}

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

	clusterName string
}

// RunProgress describes backup progress on per file basis.
//
// Each RunProgress either has Uploaded or Skipped fields set to respective
// amount of bytes. Failed shows amount of bytes that is assumed to have
// failed. Since current implementation doesn't support resume at file level
// this value will always be the same as Uploaded as file needs to be uploaded
// again. In summary Failed is supposed to mean, out of uploaded bytes how much
// bytes have to be uploaded again.
type RunProgress struct {
	ClusterID  uuid.UUID
	TaskID     uuid.UUID
	RunID      uuid.UUID
	AgentJobID int64

	Host      string
	Unit      int64
	TableName string

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

// Provider specifies type of remote storage like S3 etc.
type Provider string

// Provider enumeration.
const (
	S3    = Provider("s3")
	GCS   = Provider("gcs")
	Azure = Provider("azure")
)

func (p Provider) String() string {
	return string(p)
}

// MarshalText implements encoding.TextMarshaler.
func (p Provider) MarshalText() (text []byte, err error) {
	return []byte(p.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (p *Provider) UnmarshalText(text []byte) error {
	if s := string(text); !providers.Has(s) {
		return errors.Errorf("unrecognised provider %q", text)
	}
	*p = Provider(text)
	return nil
}

var providers = strset.New(S3.String(), GCS.String(), Azure.String())

// Location specifies storage provider and container/resource for a DC.
type Location struct {
	DC       string   `json:"dc"`
	Provider Provider `json:"provider"`
	Path     string   `json:"path"`
}

func (l Location) String() string {
	p := l.Provider.String() + ":" + l.Path
	if l.DC != "" {
		p = l.DC + ":" + p
	}
	return p
}

func (l Location) MarshalText() (text []byte, err error) {
	return []byte(l.String()), nil
}

func (l *Location) UnmarshalText(text []byte) error {
	// Providers require that resource names are DNS compliant.
	// The following is a super simplified DNS (plus provider prefix)
	// matching regexp.
	pattern := regexp.MustCompile(`^(([a-zA-Z0-9\-\_\.]+):)?([a-z0-9]+):([a-z0-9\-\.]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid location %q, the format is [dc:]<provider>:<path> ex. s3:my-bucket, the path must be DNS compliant", string(text))
	}

	if err := l.Provider.UnmarshalText(m[3]); err != nil {
		return errors.Wrapf(err, "invalid location %q", string(text))
	}

	l.DC = string(m[2])
	l.Path = string(m[4])

	return nil
}

func (l Location) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return l.MarshalText()
}

func (l *Location) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	return l.UnmarshalText(data)
}

// RemoteName returns the rclone remote name for that location.
func (l Location) RemoteName() string {
	return l.Provider.String()
}

// RemotePath returns string that can be used with rclone to specify a path in
// the given location.
func (l Location) RemotePath(p string) string {
	r := l.RemoteName()
	if r != "" {
		r += ":"
	}
	return path.Join(r+l.Path, p)
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
	Keyspace         []string   `json:"keyspace"`
	DC               []string   `json:"dc"`
	Location         []Location `json:"location"`
	Retention        int        `json:"retention"`
	RateLimit        []DCLimit  `json:"rate_limit"`
	SnapshotParallel []DCLimit  `json:"snapshot_parallel"`
	UploadParallel   []DCLimit  `json:"upload_parallel"`
	Continue         bool       `json:"continue"`
}

func defaultTaskProperties() taskProperties {
	return taskProperties{
		Retention: 3,
		Continue:  true,
	}
}
