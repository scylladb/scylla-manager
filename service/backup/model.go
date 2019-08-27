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
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/uuid"
)

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

// Run tracks backup progress, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID    uuid.UUID
	Units     []Unit
	DC        []string
	Location  []Location
	StartTime time.Time
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
	AgentJobID uuid.UUID

	Host      string
	Unit      int64
	TableName string
	FileName  string

	StartedAt   *time.Time
	CompletedAt *time.Time
	Error       string
	Size        int64 // Total file size in bytes.
	Uploaded    int64 // Amount of total uploaded bytes.
	Skipped     int64 // Amount of skipped bytes because file was present.
	// Amount of bytes that have been uploaded but due to error have to be
	// uploaded again.
	Failed int64
}

type jobStatus string

const (
	jobRunning  jobStatus = "running"
	jobError    jobStatus = "error"
	jobNotFound jobStatus = "not_found"
	jobSuccess  jobStatus = "success"
)

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

	DC    []string       `json:"dcs,omitempty"`
	Hosts []HostProgress `json:"hosts,omitempty"`
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

// TokenRangesKind enumeration.
const (
	S3 = Provider("s3")
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
	switch Provider(text) {
	case S3:
		*p = S3
	default:
		return errors.Errorf("unrecognised provider %q", text)
	}
	return nil
}

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
	pattern := regexp.MustCompile(`^(([a-z0-9\-\.]+):)?([a-z0-9]+):([a-z0-9\-\.]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid location format")
	}

	if err := l.Provider.UnmarshalText(m[3]); err != nil {
		return errors.Wrap(err, "invalid location")
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
	return path.Join(l.RemoteName()+":"+l.Path, p)
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
	pattern := regexp.MustCompile(`^(([a-z0-9\-\.]+):)?([0-9]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid format")
	}

	limit, err := strconv.ParseInt(string(m[3]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid limit")
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
