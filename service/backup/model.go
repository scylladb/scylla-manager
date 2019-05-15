// Copyright (C) 2017 ScyllaDB

package backup

import (
	"reflect"
	"regexp"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/internal/duration"
	"github.com/scylladb/mermaid/uuid"
)

// Target specifies what should be backed up and where.
type Target struct {
	Units    []Unit        `json:"units,omitempty"`
	DC       []string      `json:"dc,omitempty"`
	Location Location      `json:"location"`
	TTL      time.Duration `json:"ttl"`
}

// Unit represents keyspace and its tables.
type Unit struct {
	Keyspace  string   `json:"keyspace" db:"keyspace_name"`
	Tables    []string `json:"tables,omitempty"`
	AllTables bool     `json:"all_tables"`
}

// MarshalUDT implements UDTMarshaler.
func (u Unit) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Marshal(info, f.Interface())
}

// UnmarshalUDT implements UDTUnmarshaler.
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
	Location Location
	TTL      time.Time
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

// Location specifies storage provider and container/resource.
type Location struct {
	Provider Provider
	Path     string
}

func (l Location) String() string {
	return l.Provider.String() + ":" + l.Path
}

// MarshalText implements encoding.TextMarshaler.
func (l Location) MarshalText() (text []byte, err error) {
	return []byte(l.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *Location) UnmarshalText(text []byte) error {
	providerPath := regexp.MustCompile(`^([a-z0-9]+):([a-z0-9\-\.]+)$`)

	m := providerPath.FindSubmatch(text)
	if len(m) != 3 {
		return errors.Errorf("invalid location format")
	}

	if err := l.Provider.UnmarshalText(m[1]); err != nil {
		return errors.Wrap(err, "invalid location")
	}

	l.Path = string(m[2])

	return nil
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace  []string          `json:"keyspace"`
	DC        []string          `json:"dc"`
	Location  Location          `json:"location"`
	Retention duration.Duration `json:"retention"`
	RateLimit int64             `json:"rate_limit"`
}

func defaultTaskProperties() taskProperties {
	return taskProperties{
		Retention: duration.Duration(7 * 24 * time.Hour),
	}
}
