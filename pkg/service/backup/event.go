// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/scylla-manager/backupspec"
)

// eventMarkerKey is the log field whose presence marks a log entry as a
// purpose-built, machine-readable event rather than human-facing progress.
// Log shippers select on it, so it must not be reused for anything else.
const eventMarkerKey = "sm_event"

// eventBackupCompleted is the sm_event value emitted once per successful
// backup run, after every manifest has been moved to its final location.
const eventBackupCompleted = "backup_completed"

// manifestRef locates one node manifest written by a backup run. Together the
// refs of a single event describe the complete snapshot: a consumer can read
// every manifest without listing the bucket or reconstructing paths itself.
type manifestRef struct {
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
	DC       string `json:"dc"`
	NodeID   string `json:"node_id"`
	Path     string `json:"path"`
}

// logBackupCompleted emits a single structured entry describing a finished
// backup: which snapshot, of which cluster, and where each of its manifests
// landed.
//
// It is deliberately emitted after the stage loop rather than from
// MoveManifest, so that it means "the whole run succeeded" - a consumer can
// treat it as the point where the snapshot became complete and readable.
func (w *worker) logBackupCompleted(ctx context.Context, hosts []hostInfo) {
	refs := make([]manifestRef, 0, len(hosts))
	for _, h := range hosts {
		refs = append(refs, manifestRef{
			Provider: string(h.Location.Provider),
			Bucket:   h.Location.Path,
			DC:       h.DC,
			NodeID:   h.ID,
			Path:     backupspec.RemoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID),
		})
	}

	w.Logger.Info(ctx, "Backup completed",
		eventMarkerKey, eventBackupCompleted,
		"cluster_id", w.ClusterID.String(),
		"cluster_name", w.ClusterName,
		"task_id", w.TaskID.String(),
		"run_id", w.RunID.String(),
		"snapshot_tag", w.SnapshotTag,
		"manifests", refs,
	)
}
