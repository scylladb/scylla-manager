// Copyright (C) 2025 ScyllaDB

package tablet

import (
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target describes tables that need to be repaired.
type Target struct {
	KsTabs map[string][]string
}

// RunProgress is tablet repair progress representation in SM DB.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Keyspace  string `db:"keyspace_name"`
	Table     string `db:"table_name"`

	StartedAt   *time.Time
	CompletedAt *time.Time
	Error       string
}

func newRunProgress(clusterID, taskID, runID uuid.UUID, ks, tab string) RunProgress {
	return RunProgress{
		ClusterID: clusterID,
		TaskID:    taskID,
		RunID:     runID,
		Keyspace:  ks,
		Table:     tab,
	}
}

// Progress describes total tablet repair task progress.
type Progress struct {
	Tables []TableProgress `json:"tables"`
}

// TableProgress describes repair progress of a single table.
type TableProgress struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`

	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	Error       string     `json:"error"`
}
