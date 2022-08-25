package backup

import (
	"time"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// RestoreRun tracks restore progress, shares ID with scheduler.Run that initiated it.
type RestoreRun struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID       uuid.UUID
	ManifestPath string // marks currently processed manifest
	KeyspaceName string // marks currently processed keyspace
	TableName    string // marks currently processed table
	SnapshotTag  string
	Stage        RestoreStage
}

func (p *RestoreRunProgress) ExecDeleteQuery(s gocqlx.Session) error {
	q := table.RestoreRunProgress.DeleteQuery(s).BindStruct(p)
	return q.ExecRelease()
}

// RestoreRunProgress describes restore progress like RunProgress.
//
// If AgentJobID is present alongside with all of the above fields,
// then progress is described on per-file basis for already started
// download of SSTables with specific indexes to host.
//
// Otherwise, it only describes total size of files that need to be
// downloaded on table basis.
type RestoreRunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID

	ManifestPath string
	KeyspaceName string
	TableName    string
	Host         string // IP of the node to which SSTables are downloaded.
	AgentJobID   int64

	ManifestIP  string // IP of the node on which manifest was taken.
	SstableID   []string
	StartedAt   *time.Time
	CompletedAt *time.Time
	Error       string
	Size        int64
	Uploaded    int64
	Skipped     int64
	Failed      int64
}

// RestoreProgress groups restore progress for all backed up hosts.
// (They can belong to an already non-existent cluster).
//
// Note: Hosts are described by IP addresses, so progress information
// about scylla nodes running on the same IP are squashed into one.
type RestoreProgress struct {
	progress

	Hosts []HostProgress `json:"hosts,omitempty"`
	Stage RestoreStage   `json:"stage"`
}
