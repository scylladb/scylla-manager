package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func (w *restoreWorker) restoreFiles(ctx context.Context, run *RestoreRun, target RestoreTarget, localDC string) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)

	filter, err := ksfilter.NewFilter(target.Keyspace)
	if err != nil {
		return errors.Wrap(err, "crete filter")
	}

	// Set to true if current run has already skipped all tables processed in previous run
	var resumed bool
	if !target.Continue || run.PrevID == uuid.Nil {
		resumed = true
	}

	// loop locations
	for _, l := range target.Location {
		w.Logger.Info(ctx, "Looping locations",
			"cluster_id", w.ClusterID,
			"location", l,
		)

		hosts, err := w.initRestoreHostPool(ctx, run, []string{l.DC, localDC}, resumed)
		if err != nil {
			return errors.Wrap(err, "initialize host pool")
		}

		// Loop manifests for the snapshot tag
		err = w.forEachRestoredManifest(ctx, l, func(miwc ManifestInfoWithContent) error {
			w.Logger.Info(ctx, "Looping manifests",
				"cluster_id", w.ClusterID,
				"location", l,
				"manifest", miwc.ManifestInfo,
			)

			// Check if manifest has already been processed in previous run
			if !resumed && run.ManifestPath != miwc.Path() {
				return nil
			} else {
				run.ManifestPath = miwc.Path()
			}

			// Represents error that occurred while looping tables
			var indexErr error
			// Loop tables for the manifest
			err = miwc.ForEachIndexIter(func(fm FilesMeta) {
				// Skip system, filtered out or empty tables
				if isSystemKeyspace(fm.Keyspace) || !filter.Check(fm.Keyspace, fm.Table) || len(fm.Files) == 0 {
					return
				}
				// In case of an error from previous iteration, do not proceed
				if indexErr != nil {
					return
				}

				w.Logger.Info(ctx, "Looping tables",
					"cluster_id", w.ClusterID,
					"location", l,
					"manifest_path", run.ManifestPath,
					"keyspace", fm.Keyspace,
					"table", fm.Table,
				)

				bundles := groupSSTablesToBundles(fm.Files)
				w.Logger.Info(ctx, "Grouped files to bundles",
					"bundles", bundles,
				)

				// Represents progress of the current table from previous run
				var tablePr tableRunProgress
				if !resumed {
					// Check if table has already been processed in previous run
					if run.KeyspaceName != fm.Keyspace || run.TableName != fm.Table {
						return
					}
					// Last run ended it's work on this table
					resumed = true

					tablePr, err = w.getTableRunProgress(ctx, run)
					if err != nil {
						indexErr = err
						return
					}
				} else {
					run.TableName = fm.Table
					run.KeyspaceName = fm.Keyspace

					w.InsertRun(ctx, run)
				}

				bundleIDPool := initBundlePool(tablePr, bundles)
				if len(bundleIDPool) == 0 {
					w.Logger.Info(ctx, "No more bundles to restore",
						"keyspace", run.KeyspaceName,
						"table", run.TableName,
					)

					return
				}

				srcDir := l.RemotePath(miwc.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version))

				version, err := w.RecordTableVersion(ctx, w.clusterSession, fm.Keyspace, fm.Table)
				if err != nil {
					indexErr = err
					return
				}

				dstDir := uploadTableDir(fm.Keyspace, fm.Table, version)

				err = w.ExecOnDisabledTable(ctx, w.clusterSession, fm.Keyspace, fm.Table, func() error {
					// TODO: change it to work in parallel
					for {
						// Get host from the pool
						var h jobUnit
						select {
						case <-ctx.Done():
							return ctx.Err()
						case h = <-hosts:
						default:
						}

						if h.Host == "" {
							w.Logger.Info(ctx, "No more hosts in the pool",
								"keyspace", run.KeyspaceName,
								"table", run.TableName,
							)

							break
						}

						if ctx.Err() != nil {
							return ctx.Err()
						}

						var (
							pr    *RestoreRunProgress
							batch []string
						)

						// Check if host has an already running job
						if h.JobID == 0 {
							takenIDs, err := w.chooseIDsForBatch(ctx, h.Host, target, bundleIDPool)
							if err != nil {
								w.Logger.Info(ctx, "Couldn't create batch for restore",
									"host", h.Host,
									"keyspace", fm.Keyspace,
									"table", fm.Table,
									"error", err,
								)

								continue
							}
							if takenIDs == nil {
								w.Logger.Info(ctx, "Empty batch")

								hosts <- jobUnit{Host: h.Host}

								break
							}

							batch = batchFromIDs(bundles, takenIDs)

							w.Logger.Info(ctx, "Created batch",
								"host", h.Host,
								"keyspace", fm.Keyspace,
								"table", fm.Table,
								"batch", batch,
							)

							jobID, err := w.Client.RcloneCopyPaths(ctx, h.Host, dstDir, srcDir, batch)
							if err != nil {
								w.Logger.Error(ctx, "Couldn't download batch to upload dir",
									"host", h.Host,
									"srcDir", srcDir,
									"dstDir", dstDir,
									"batch", batch,
									"error", err,
								)
								// Return bundle IDs to the pool so that they can be used in different batch
								returnBatchToPool(bundleIDPool, takenIDs)

								continue
							}

							pr = &RestoreRunProgress{
								ClusterID:    run.ClusterID,
								TaskID:       run.TaskID,
								RunID:        run.ID,
								ManifestPath: run.ManifestPath,
								KeyspaceName: fm.Keyspace,
								TableName:    fm.Table,
								Host:         h.Host,
								AgentJobID:   jobID,
								ManifestIP:   miwc.IP,
								SstableID:    takenIDs,
							}
						} else {
							pr = tablePr[h.JobID]
							batch = batchFromIDs(bundles, pr.SstableID)
						}

						if err := w.waitJob(ctx, pr); err != nil {
							// In case of context cancellation restore is interrupted
							if ctx.Err() != nil {
								return ctx.Err()
							}

							w.Logger.Error(ctx, "Couldn't wait on Rclone job",
								"host", h.Host,
								"job ID", h.JobID,
								"error", err,
							)
							// Since failed progress run might already be recorded in database it has to be deleted
							w.DeleteRunProgress(ctx, pr)
							returnBatchToPool(bundleIDPool, pr.SstableID)

							continue
						}

						if err := w.Client.Restore(ctx, h.Host, fm.Keyspace, fm.Table, version, batch); err != nil {
							w.DeleteRunProgress(ctx, pr)
							returnBatchToPool(bundleIDPool, pr.SstableID)

							continue
						}

						// Return free host to the pool so that it can be reused
						hosts <- jobUnit{Host: h.Host}
					}

					return nil
				})

				if err != nil {
					indexErr = err

					return
				}

				if len(bundleIDPool) > 0 {
					w.Logger.Error(ctx, "Not all bundles have been restored",
						"left_bundles", getChanContent(bundleIDPool),
					)
				}

			})

			if indexErr != nil {
				return indexErr
			}
			return err
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (w *restoreWorker) waitJob(ctx context.Context, pr *RestoreRunProgress) (err error) {
	defer func() {
		// Running stop procedure in a different context because original may be canceled
		stopCtx := context.Background()

		// On error stop job
		if err != nil {
			w.Logger.Info(ctx, "Stop job", "host", pr.Host, "id", pr.AgentJobID)
			if e := w.Client.RcloneJobStop(stopCtx, pr.Host, pr.AgentJobID); e != nil {
				w.Logger.Error(ctx, "Failed to stop job",
					"host", pr.Host,
					"id", pr.AgentJobID,
					"error", e,
				)
			}
		}

		// On exit clear stats
		if e := w.clearJobStats(stopCtx, pr.AgentJobID, pr.Host); e != nil {
			w.Logger.Error(ctx, "Failed to clear job stats",
				"host", pr.Host,
				"id", pr.AgentJobID,
				"error", e,
			)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			job, err := w.Client.RcloneJobProgress(ctx, pr.Host, pr.AgentJobID, w.Config.LongPollingTimeoutSeconds)
			if err != nil {
				return errors.Wrap(err, "fetch job info")
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			switch scyllaclient.RcloneJobStatus(job.Status) {
			case scyllaclient.JobError:
				return errors.Errorf("job error (%d): %s", pr.AgentJobID, job.Error)
			case scyllaclient.JobSuccess:
				w.updateProgress(ctx, pr, job)
				return nil
			case scyllaclient.JobRunning:
				w.updateProgress(ctx, pr, job)
			case scyllaclient.JobNotFound:
				return errJobNotFound
			}
		}
	}
}

func (w *restoreWorker) updateProgress(ctx context.Context, pr *RestoreRunProgress, job *scyllaclient.RcloneJobProgress) {
	pr.StartedAt = nil
	// Set StartedAt and CompletedAt based on Job
	if t := time.Time(job.StartedAt); !t.IsZero() {
		pr.StartedAt = &t
	}
	pr.CompletedAt = nil
	if t := time.Time(job.CompletedAt); !t.IsZero() {
		pr.CompletedAt = &t
	}

	pr.Error = job.Error
	pr.Uploaded = job.Uploaded
	pr.Skipped = job.Skipped
	pr.Failed = job.Failed
	// TODO: fix restore metrics.
	w.Metrics.SetFilesProgress(w.ClusterID, pr.KeyspaceName, pr.TableName, pr.Host,
		pr.Size, pr.Uploaded, pr.Skipped, pr.Failed)

	w.InsertRunProgress(ctx, pr)
}
