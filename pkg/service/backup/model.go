// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/multierr"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// SnapshotInfo contains detailed information about snapshot.
type SnapshotInfo struct {
	SnapshotTag string `json:"snapshot_tag"`
	Nodes       int    `json:"nodes"`
	Size        int64  `json:"size"`
}

// SnapshotInfoList slice of SnapshotInfo.
type SnapshotInfoList []SnapshotInfo

// GetOrAppend returns pointer to the item that has provided snapshotTag, if it is not present adds it to the list.
func (l *SnapshotInfoList) GetOrAppend(snapshotTag string) *SnapshotInfo {
	for id := range *l {
		item := &(*l)[id]
		if item.SnapshotTag == snapshotTag {
			return item
		}
	}
	*l = append(*l, SnapshotInfo{
		SnapshotTag: snapshotTag,
	})
	return &(*l)[len(*l)-1]
}

// ListItem represents contents of a snapshot within list boundaries.
type ListItem struct {
	ClusterID    uuid.UUID        `json:"cluster_id"`
	TaskID       uuid.UUID        `json:"task_id"`
	Units        []Unit           `json:"units"`
	SnapshotInfo SnapshotInfoList `json:"snapshot_info"`

	unitCache map[string]*strset.Set `json:"-"`
}

// ListItems is a slice of ListItem.
type ListItems []*ListItem

// GetOrAppend returns item that has provided clusterID and taskID, if it is not present adds it.
func (l *ListItems) GetOrAppend(clusterID, taskID uuid.UUID) *ListItem {
	for _, item := range *l {
		if item.ClusterID == clusterID && item.TaskID == taskID {
			return item
		}
	}
	item := &ListItem{
		ClusterID:    clusterID,
		TaskID:       taskID,
		Units:        nil,
		SnapshotInfo: []SnapshotInfo{},
	}
	*l = append(*l, item)
	return item
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

// RemoveSystemTables removes tables that belongs to system_schema since they are too version-mobile
// which is bad for testing purposes.
func (t Target) RemoveSystemTables() {
	for id := range t.Units {
		if t.Units[id].Keyspace == "system_schema" {
			t.Units[id].Tables = nil
		}
	}
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

type fileInfo struct {
	Name string
	Size int64
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

// TaskProperties is the main data structure of the runner.Properties blob.
type TaskProperties struct {
	Keyspace         []string     `json:"keyspace"`
	DC               []string     `json:"dc"`
	Location         []Location   `json:"location"`
	Retention        *int         `json:"retention"`
	RetentionDays    *int         `json:"retention_days"`
	RetentionMap     RetentionMap `json:"retention_map"`
	RateLimit        []DCLimit    `json:"rate_limit"`
	SnapshotParallel []DCLimit    `json:"snapshot_parallel"`
	UploadParallel   []DCLimit    `json:"upload_parallel"`
	Continue         bool         `json:"continue"`
	PurgeOnly        bool         `json:"purge_only"`
}

func (p TaskProperties) extractRetention() RetentionPolicy {
	if p.Retention == nil && p.RetentionDays == nil {
		return defaultRetention()
	}

	var r RetentionPolicy
	if p.Retention != nil {
		r.Retention = *p.Retention
	}
	if p.RetentionDays != nil {
		r.RetentionDays = *p.RetentionDays
	}
	return r
}

func defaultTaskProperties() TaskProperties {
	return TaskProperties{
		Continue: true,
	}
}

func extractLocations(properties []json.RawMessage) ([]Location, error) {
	var (
		m         = strset.New()
		locations []Location
		errs      error
	)

	for i := range properties {
		var p TaskProperties
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

// GetTasksProperties extract TaskProperties from specified Tasks array.
func GetTasksProperties(tasks []*scheduler.TaskListItem) (TaskPropertiesByUUID, error) {
	var errs []error
	propertiesByTaskIds := make(TaskPropertiesByUUID)
	for _, task := range tasks {
		var properties TaskProperties
		if err := json.Unmarshal(task.Properties, &properties); err != nil {
			errs = append(errs, err)
			continue
		}
		propertiesByTaskIds[task.ID] = &properties
	}
	return propertiesByTaskIds, stdErrors.Join(errs...)
}

// TaskPropertiesByUUID represent map with uuid as key and reference on TaskProperties as value.
type TaskPropertiesByUUID map[uuid.UUID]*TaskProperties

// GetLocations return all tasks locations exclude duplicates.
func (p TaskPropertiesByUUID) GetLocations() Locations {
	locations := make(Locations, 0)
	for _, taskProp := range p {
		for _, loc := range taskProp.Location {
			if !locations.Contains(loc.Provider, loc.Path) {
				locations = append(locations, loc)
			}
		}
	}
	return locations
}

// GetRetentionMap return combined RetentionMap for all tasks.
func (p TaskPropertiesByUUID) GetRetentionMap() RetentionMap {
	retentionMap := make(RetentionMap)
	for taskID, taskProp := range p {
		retentionMap[taskID] = taskProp.RetentionMap[taskID]
	}
	return retentionMap
}

// RetentionPolicy defines the retention policy for a backup task.
type RetentionPolicy struct {
	RetentionDays int `json:"retention_days"`
	Retention     int `json:"retention"`
}

func defaultRetention() RetentionPolicy {
	return RetentionPolicy{
		Retention:     3,
		RetentionDays: 0,
	}
}

func defaultRetentionForDeletedTask() RetentionPolicy {
	return RetentionPolicy{
		Retention:     0,
		RetentionDays: 30,
	}
}

// RetentionMap is a mapping of TaskIDs to retention policies.
type RetentionMap map[uuid.UUID]RetentionPolicy

// Contains return true if specified taskID is in map.
func (r RetentionMap) Contains(taskID uuid.UUID) bool {
	_, ok := r[taskID]
	return ok
}

// Add RetentionPolicy and return true if specified taskID is not in map.
func (r RetentionMap) Add(taskID uuid.UUID, retention, days int) bool {
	if r.Contains(taskID) {
		return false
	}

	r[taskID] = RetentionPolicy{
		Retention:     retention,
		RetentionDays: days,
	}

	return true
}

// GetPolicy return retention policy for a given task ID. If missing - return default policy.
func (r RetentionMap) GetPolicy(taskID uuid.UUID) RetentionPolicy {
	if policy, ok := r[taskID]; ok {
		return policy
	}
	return defaultRetentionForDeletedTask()
}

// PolicyExists return if policy exists for such taskID.
func (r RetentionMap) PolicyExists(taskID uuid.UUID) bool {
	_, ok := r[taskID]
	return ok
}

// ExtractRetention parses properties as task properties and returns "retention".
func ExtractRetention(properties json.RawMessage) (RetentionPolicy, error) {
	var p TaskProperties
	if err := json.Unmarshal(properties, &p); err != nil {
		return RetentionPolicy{}, err
	}
	return p.extractRetention(), nil
}
