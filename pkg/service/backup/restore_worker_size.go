package backup

import (
	"context"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// RecordSize records size of every table from every manifest.
// Resuming is implemented on manifest (nodeID) level.
func (w *restoreWorker) RecordSize(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	var resumed bool
	if !target.Continue || run.PrevID == uuid.Nil {
		resumed = true
	}

	pr := &RestoreRunProgress{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		RunID:     run.ID,
	}

	for _, l := range target.Location {
		err := w.forEachRestoredManifest(ctx, l, func(miwc ManifestInfoWithContent) error {
			if !resumed {
				if run.ManifestPath != miwc.Path() {
					return nil
				}
				resumed = true
			} else {
				run.ManifestPath = miwc.Path()
				// Record size calculation progress
				w.InsertRun(ctx, run)
			}

			pr.ManifestPath = run.ManifestPath
			pr.ManifestIP = miwc.IP

			return miwc.ForEachIndexIter(func(fm FilesMeta) {
				pr.KeyspaceName = fm.Keyspace
				pr.TableName = fm.Table
				pr.Size = fm.Size
				// Record table's size

				w.Logger.Info(ctx, "Recorded table size",
					"manifest", pr.ManifestPath,
					"keyspace", pr.KeyspaceName,
					"table", pr.TableName,
					"size", pr.Size,
				)

				w.InsertRunProgress(ctx, pr)
			})
		})
		if err != nil {
			return err
		}
	}

	return nil
}
