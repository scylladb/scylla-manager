// Copyright (C) 2017 ScyllaDB

package backup

import (
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/uuid"
)

// Target specifies what should be backed up and where.
type Target struct {
	Units     []Unit      `json:"units,omitempty"`
	DC        []string    `json:"dc,omitempty"`
	Location  []Location  `json:"location"`
	Retention int         `json:"retention"`
	RateLimit []RateLimit `json:"rate_limit"`
}

// Unit represents keyspace and its tables.
type Unit struct {
	Keyspace string   `json:"keyspace" db:"keyspace_name"`
	Tables   []string `json:"tables,omitempty"`
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

	PrevID   uuid.UUID
	Units    []Unit
	DC       []string
	Location []Location
}

// RunProgress describes backup progress on per file basis.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	Host  string
	Unit  int
	Table int `db:"table_id"`

	Size     int64
	Uploaded int64
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

// RateLimit specifies a rate limit for a DC.
type RateLimit struct {
	DC    string `json:"dc"`
	Limit int    `json:"limit"`
}

func (r RateLimit) String() string {
	p := fmt.Sprint(r.Limit)
	if r.DC != "" {
		p = r.DC + ":" + p
	}
	return p
}

func (r RateLimit) MarshalText() (text []byte, err error) {
	return []byte(r.String()), nil
}

func (r *RateLimit) UnmarshalText(text []byte) error {
	pattern := regexp.MustCompile(`^(([a-z0-9\-\.]+):)?([0-9]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid location format")
	}

	limit, err := strconv.ParseInt(string(m[3]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid limit")
	}

	r.DC = string(m[2])
	r.Limit = int(limit)

	return nil
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace  []string    `json:"keyspace"`
	DC        []string    `json:"dc"`
	Location  []Location  `json:"location"`
	Retention int         `json:"retention"`
	RateLimit []RateLimit `json:"rate_limit"`
}

func defaultTaskProperties() taskProperties {
	return taskProperties{
		Retention: 3,
	}
}
