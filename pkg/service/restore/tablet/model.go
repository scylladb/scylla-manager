// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// RunProgress is tablet aware restore progress representation in SM DB.
type RunProgress struct {
	// Partition key
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	// Clustering key
	Keyspace string `db:"keyspace_name"`
	Table    string `db:"table_name"`

	// Host on which tablet aware restore task was scheduled.
	// Task status can be queried only on this host.
	Host         string
	ScyllaTaskID string `db:"scylla_task_id"`
	StartedAt    time.Time
	CompletedAt  time.Time
	// Scylla reports progress in restored and total sstables
	RestoredSSTables int64 `db:"restored_sstables"`
	TotalSSTables    int64 `db:"total_sstables"`
	// Size of the restored table taken from manifest
	Size  int64
	Error string
}

// isSuccess reports whether the table was fully restored without an error.
func (pr RunProgress) isSuccess() bool {
	return !pr.CompletedAt.IsZero() && pr.Error == ""
}

// canReattach reports whether we can try to re-attach to the
// ongoing tablet aware restore task. It allows for smoother
// resume on SM crash. It won't work on resume after task pause,
// as it results in aborting scylla tasks.
func (pr RunProgress) canReattach() bool {
	return pr.ScyllaTaskID != "" && pr.Host != "" && pr.CompletedAt.IsZero() && pr.Error == ""
}
