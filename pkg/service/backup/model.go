// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"

	"go.uber.org/multierr"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// SnapshotInfo contains detailed information about backupspec.
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
	Units            []Unit                `json:"units,omitempty"`
	DC               []string              `json:"dc,omitempty"`
	Location         []backupspec.Location `json:"location"`
	Retention        int                   `json:"retention"`
	RetentionDays    int                   `json:"retention_days"`
	RetentionMap     RetentionMap          `json:"-"` // policy for all tasks, injected in runtime
	RateLimit        []DCLimit             `json:"rate_limit,omitempty"`
	Transfers        int                   `json:"transfers"`
	SnapshotParallel []DCLimit             `json:"snapshot_parallel,omitempty"`
	UploadParallel   []DCLimit             `json:"upload_parallel,omitempty"`
	Continue         bool                  `json:"continue,omitempty"`
	PurgeOnly        bool                  `json:"purge_only,omitempty"`
	SkipSchema       bool                  `json:"skip_schema,omitempty"`

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
	Location    []backupspec.Location
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

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace         []string              `json:"keyspace"`
	DC               []string              `json:"dc"`
	Location         []backupspec.Location `json:"location"`
	Retention        *int                  `json:"retention"`
	RetentionDays    *int                  `json:"retention_days"`
	RetentionMap     RetentionMap          `json:"retention_map"`
	RateLimit        []DCLimit             `json:"rate_limit"`
	Transfers        int                   `json:"transfers"`
	SnapshotParallel []DCLimit             `json:"snapshot_parallel"`
	UploadParallel   []DCLimit             `json:"upload_parallel"`
	Continue         bool                  `json:"continue"`
	PurgeOnly        bool                  `json:"purge_only"`
	SkipSchema       bool                  `json:"skip_schema"`
}

func (p taskProperties) validate(dcs []string, dcMap map[string][]string) error {
	if p.Location == nil {
		return errors.Errorf("missing location")
	}
	if policy := p.extractRetention(); policy.Retention < 0 || policy.RetentionDays < 0 {
		return errors.New("negative retention")
	}
	if p.Transfers != scyllaclient.TransfersFromConfig && p.Transfers < 1 {
		return errors.New("transfers param has to be equal to -1 (set transfers to the value from scylla-manager-agent.yaml config) " +
			"or greater than zero")
	}

	// Validate location DCs
	if err := CheckDCs(p.Location, dcMap); err != nil {
		return errors.Wrap(err, "invalid location")
	}
	// Validate rate limit DCs
	if err := CheckDCs(p.RateLimit, dcMap); err != nil {
		return errors.Wrap(err, "invalid rate-limit")
	}
	// Validate upload parallel DCs
	if err := CheckDCs(p.SnapshotParallel, dcMap); err != nil {
		return errors.Wrap(err, "invalid snapshot-parallel")
	}
	// Validate snapshot parallel DCs
	if err := CheckDCs(p.UploadParallel, dcMap); err != nil {
		return errors.Wrap(err, "invalid upload-parallel")
	}
	// Validate all DCs have backup location
	if err := CheckAllDCsCovered(FilterDCs(p.Location, dcs), dcs); err != nil {
		return errors.Wrap(err, "invalid location")
	}
	return nil
}

func (p taskProperties) toTarget(ctx context.Context, client *scyllaclient.Client, dcs []string,
	liveNodes scyllaclient.NodeStatusInfoSlice, filters []tabFilter, validators []tabValidator,
) (Target, error) {
	policy := p.extractRetention()
	rateLimit := FilterDCs(p.RateLimit, dcs)
	if len(rateLimit) == 0 {
		rateLimit = []DCLimit{{Limit: defaultRateLimit}}
	}

	units, err := p.createUnits(ctx, client, filters, validators)
	if err != nil {
		return Target{}, errors.Wrap(err, "create units")
	}

	return Target{
		Units:            units,
		DC:               dcs,
		Location:         FilterDCs(p.Location, dcs),
		Retention:        policy.Retention,
		RetentionDays:    policy.RetentionDays,
		RetentionMap:     p.RetentionMap,
		RateLimit:        rateLimit,
		Transfers:        p.Transfers,
		SnapshotParallel: FilterDCs(p.SnapshotParallel, dcs),
		UploadParallel:   FilterDCs(p.UploadParallel, dcs),
		Continue:         p.Continue,
		PurgeOnly:        p.PurgeOnly,
		SkipSchema:       p.SkipSchema,
		liveNodes:        liveNodes,
	}, nil
}

func (p taskProperties) createUnits(ctx context.Context, client *scyllaclient.Client,
	filters []tabFilter, validators []tabValidator,
) ([]Unit, error) {
	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "get keyspaces")
	}
	rd := scyllaclient.NewRingDescriber(ctx, client)

	var units []Unit
	for _, ks := range keyspaces {
		tables, err := client.Tables(ctx, ks)
		if err != nil {
			return nil, errors.Wrapf(err, "keyspace %s: get tables", ks)
		}
		// Before Scylla 6.0, schema had to be restored from sstables
		// (output of DESC SCHEMA was missing important information like dropped columns).
		// Starting from Scylla 6.0, schema has to be restored from output of DESC SCHEMA WITH INTERNALS
		// (restoring sstables doesn't play well with raft).
		// system_schema sstables can still be backed up - just in case.
		if ks == systemSchema {
			if !p.SkipSchema {
				units = append(units, Unit{
					Keyspace:  ks,
					Tables:    tables,
					AllTables: true,
				})
			}
			continue
		}

		var filteredTables []string
		for _, tab := range tables {
			ring, err := rd.DescribeRing(ctx, ks, tab)
			if err != nil {
				return nil, errors.Wrap(err, "describe ring")
			}

			// Apply filters
			skip := false
			for _, f := range filters {
				if !f.filter(ks, tab, ring) {
					skip = true
				}
			}
			if skip {
				continue
			}

			// Apply validators
			for _, v := range validators {
				if err := v.validate(ks, tab, ring); err != nil {
					return nil, err
				}
			}

			filteredTables = append(filteredTables, tab)
		}

		if len(filteredTables) > 0 {
			units = append(units, Unit{
				Keyspace:  ks,
				Tables:    filteredTables,
				AllTables: len(filteredTables) == len(tables),
			})
		}
	}

	// Validate that any keyspace except for system_schema is going to be backed up
	nonSchemaUnitCnt := len(units)
	if !p.SkipSchema {
		nonSchemaUnitCnt--
	}
	if nonSchemaUnitCnt <= 0 {
		return nil, errors.New("no keyspace matched criteria")
	}

	sortUnits(units)
	return units, nil
}

func (p taskProperties) extractRetention() RetentionPolicy {
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

func defaultTaskProperties() taskProperties {
	return taskProperties{
		Transfers: scyllaclient.TransfersFromConfig,
		Continue:  true,
	}
}

func extractLocations(properties []json.RawMessage) ([]backupspec.Location, error) {
	var (
		m         = strset.New()
		locations []backupspec.Location
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

// ExtractRetention parses properties as task properties and returns "retention".
func ExtractRetention(properties json.RawMessage) (RetentionPolicy, error) {
	var p taskProperties
	if err := json.Unmarshal(properties, &p); err != nil {
		return RetentionPolicy{}, err
	}
	return p.extractRetention(), nil
}

// tabFilter checks if table should be backed.
type tabFilter interface {
	filter(ks, tab string, ring scyllaclient.Ring) bool
}

// Filters tables according to '--keyspace' flag.
type patternFilter struct {
	pattern *ksfilter.Filter
}

func (f patternFilter) filter(ks, tab string, _ scyllaclient.Ring) bool {
	return f.pattern.Check(ks, tab)
}

// Filters out tables not replicated in backed up dcs.
type dcFilter struct {
	dcs *strset.Set
}

func (f dcFilter) filter(_, _ string, ring scyllaclient.Ring) bool {
	return f.dcs.HasAny(ring.Datacenters()...)
}

// Filters out tables containing local data, as they shouldn't be restored.
type localDataFilter struct {
	keyspaces map[scyllaclient.KeyspaceType][]string
}

func (f localDataFilter) filter(ks, _ string, _ scyllaclient.Ring) bool {
	user, okU := f.keyspaces[scyllaclient.KeyspaceTypeUser]
	nonLocal, okNL := f.keyspaces[scyllaclient.KeyspaceTypeNonLocal]
	if !okU || !okNL {
		panic(fmt.Sprintf("keyspace map %v does not contain expected entries %s and %s",
			f.keyspaces, scyllaclient.KeyspaceTypeUser, scyllaclient.KeyspaceTypeNonLocal))
	}
	return slices.Contains(user, ks) || slices.Contains(nonLocal, ks)
}

// Filters out views as they are restored by re-creating them on restored base table.
// There is no use in backing up their sstables.
type viewFilter struct {
	views *strset.Set
}

func (f viewFilter) filter(ks, tab string, _ scyllaclient.Ring) bool {
	return !f.views.Has(ks + "." + tab)
}

// tableValidator checks if it's safe to back up table.
type tabValidator interface {
	validate(ks, tab string, ring scyllaclient.Ring) error
}

// Validates that each token range is owned by at least one live backed up node.
// Otherwise, corresponding data wouldn't be included in the backup.
type tokenRangesValidator struct {
	liveNodes *map[netip.Addr]struct{}
	dcs       *strset.Set
}

func (v tokenRangesValidator) validate(ks, tab string, ring scyllaclient.Ring) error {
	for _, rt := range ring.ReplicaTokens {
		if !maps.HasAnyKey(*v.liveNodes, rt.ReplicaSet...) {
			return errors.Errorf("%s.%s: the whole replica set %v is filtered out, so the data owned by it can't be backed up", ks, tab, rt.ReplicaSet)
		}
	}
	return nil
}

func sortUnits(units []Unit) {
	slices.SortFunc(units, func(a, b Unit) int {
		l := strings.HasPrefix(a.Keyspace, "system")
		r := strings.HasPrefix(b.Keyspace, "system")
		// Put system_schema at the end as schema mustn't be older than snapshot-ed data.
		switch {
		case b.Keyspace == systemSchema || l && !r:
			return -1
		case a.Keyspace == systemSchema || !l && r:
			return 1
		default:
			if a.Keyspace < b.Keyspace {
				return -1
			}
			return 1
		}
	})
}
