// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []backupspec.Location `json:"location"`
	Keyspace        []string              `json:"keyspace,omitempty"`
	SourceClusterID uuid.UUID             `json:"source_cluster_id"`
	SnapshotTag     string                `json:"snapshot_tag"`
	NodesMapping    []nodeMapping         `json:"nodes_mapping"`
}

func defaultTarget() Target {
	return Target{
		Keyspace: []string{"*"},
	}
}

type nodeMapping struct {
	Source node `json:"source"`
	Target node `json:"target"`
}

type node struct {
	DC     string `json:"dc"`
	Rack   string `json:"rack"`
	HostID string `json:"host_id"`
}

// Host contains basic information about Scylla node.
type Host struct {
	ID         string
	DC         string
	Addr       string
	ShardCount int
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
	Keyspace    string                       `json:"keyspace" db:"keyspace_name"`
	View        string                       `json:"view" db:"view_name"`
	Type        ViewType                     `json:"type" db:"view_type"`
	BaseTable   string                       `json:"base_table"`
	CreateStmt  string                       `json:"create_stmt,omitempty"`
	BuildStatus scyllaclient.ViewBuildStatus `json:"status"`
}

// hostWorkload represents what data (manifest) from the backup should be handled
// by which node (host) in the target cluster.
type hostWorkload struct {
	host            Host
	manifestInfo    *backupspec.ManifestInfo
	manifestContent *backupspec.ManifestContentWithIndex

	tablesToRestore []scyllaTableWithSize
}

type scyllaTableWithSize struct {
	scyllaTable
	size int64
}

type scyllaTable struct{ keyspace, table string }

func (st scyllaTable) String() string {
	return st.keyspace + "." + st.table
}

func getTablesToRestore(workload []hostWorkload) map[scyllaTable]struct{} {
	tablesToRestore := map[scyllaTable]struct{}{}
	for _, wl := range workload {
		for _, table := range wl.tablesToRestore {
			tablesToRestore[table.scyllaTable] = struct{}{}
		}
	}
	return tablesToRestore
}

func (t *Target) validateProperties(keyspaces []string) error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if !backupspec.IsSnapshotTag(t.SnapshotTag) {
		return errors.Errorf("unexpected snapshot-tag format: %s", t.SnapshotTag)
	}
	if t.SourceClusterID == uuid.Nil {
		return errors.New("source cluster id is empty")
	}
	if err := validateKeyspaceFilter(t.Keyspace, keyspaces); err != nil {
		return errors.Wrap(err, "keyspace filter")
	}
	if err := validateNodesMapping(t.NodesMapping); err != nil {
		return errors.Wrap(err, "nodes mapping")
	}
	return nil
}

// 1-1-restore --keyspace filter is limited to keyspaces only (e.g. keyspace.table is not supported).
func validateKeyspaceFilter(keyspaceFilter, keyspaces []string) error {
	// default value, it's ok to have a wildcard(*) in that case.
	if len(keyspaceFilter) == 1 && keyspaceFilter[0] == "*" {
		return nil
	}
	clusterKeyspaces := strset.New(keyspaces...)
	for _, filter := range keyspaceFilter {
		if !clusterKeyspaces.Has(filter) {
			return errors.Errorf("only existing keyspaces can be provided, but got: %s", filter)
		}
	}
	return nil
}

type dcRack struct {
	dc   string
	rack string
}

func validateNodesMapping(nodesMapping []nodeMapping) error {
	if len(nodesMapping) == 0 {
		return errors.New("empty")
	}

	var (
		sourceDCMap = map[dcRack]dcRack{}
		targetDCMap = map[dcRack]dcRack{}

		sourceDCRackMap = map[dcRack]dcRack{}
		targetDCRackMap = map[dcRack]dcRack{}

		sourceNodes = map[string]struct{}{}
		targetNodes = map[string]struct{}{}
	)

	for _, nodeMapping := range nodesMapping {
		s, t := nodeMapping.Source, nodeMapping.Target

		// Check DCs
		if err := checkDCRackMapping(sourceDCMap, dcRack{dc: s.DC}, dcRack{dc: t.DC}); err != nil {
			return err
		}
		if err := checkDCRackMapping(targetDCMap, dcRack{dc: t.DC}, dcRack{dc: s.DC}); err != nil {
			return err
		}
		// Check Racks
		sourceDCRack, targetDCRack := dcRack{dc: s.DC, rack: s.Rack}, dcRack{dc: t.DC, rack: t.Rack}
		if err := checkDCRackMapping(sourceDCRackMap, sourceDCRack, targetDCRack); err != nil {
			return err
		}
		if err := checkDCRackMapping(targetDCRackMap, targetDCRack, sourceDCRack); err != nil {
			return err
		}
		// Check Hosts
		if err := checkHostMapping(sourceNodes, s.HostID); err != nil {
			return err
		}
		if err := checkHostMapping(targetNodes, t.HostID); err != nil {
			return err
		}
	}
	return nil
}

func checkDCRackMapping(dcRackMap map[dcRack]dcRack, source, target dcRack) error {
	mapped, ok := dcRackMap[source]
	if !ok {
		dcRackMap[source] = target
		return nil
	}
	if mapped != target {
		return errors.Errorf("%s %s is already mapped to %s %s", source.dc, source.rack, mapped.dc, mapped.rack)
	}
	return nil
}

func checkHostMapping(hostMap map[string]struct{}, hostID string) error {
	if _, ok := hostMap[hostID]; !ok {
		hostMap[hostID] = struct{}{}
		return nil
	}
	return errors.Errorf("host is already mapped: %s", hostID)
}

// RunTableProgress database representation for table progress.
type RunTableProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	StartedAt   *time.Time
	CompletedAt *time.Time

	KeyspaceName string
	TableName    string
	Error        string

	Host string

	TableSize           int64
	Downloaded          int64
	VersionedDownloaded int64
	IsRefreshed         bool // indicates whether node tool refresh is completed for this table or not
}

// RunViewProgress database representation of view progress.
type RunViewProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	StartedAt   *time.Time
	CompletedAt *time.Time

	KeyspaceName string
	TableName    string
	Error        string

	ViewType        string
	ViewBuildStatus scyllaclient.ViewBuildStatus
}

// Progress groups restore progress for all restored keyspaces.
type Progress struct {
	Tables []TableProgress `json:"tables"`
	Views  []ViewProgress  `json:"views"`
}

// TableProgress defines restore progress for the table.
type TableProgress struct {
	progress

	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
}

// ViewProgress defines restore progress for the view.
type ViewProgress struct {
	progress

	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	ViewType string `json:"type"`
}

// progress describes general properties of Table or View progress.
type progress struct {
	StartedAt   *time.Time     `json:"started_at"`
	CompletedAt *time.Time     `json:"completed_at"`
	Size        int64          `json:"size"`
	Restored    int64          `json:"restored"`
	Status      ProgressStatus `json:"status"`
}

// ProgressStatus enum for progress status.
type ProgressStatus string

var (
	// ProgressStatusNotStarted indicates that 1-1-restore of table/view is not yet started.
	ProgressStatusNotStarted ProgressStatus = "not_started"
	// ProgressStatusInProgress indicates that 1-1-restore of table/view is in progress.
	ProgressStatusInProgress ProgressStatus = "in_progress"
	// ProgressStatusDone indicates that 1-1-restore of table/view is done.
	ProgressStatusDone ProgressStatus = "done"
)
