package backup

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// tableRunProgress maps rclone job ID to corresponding RestoreProgressRun
// for a specific table.
type tableRunProgress map[int64]*RestoreRunProgress

// jobUnit represents host that can be used for restoring files.
// If set, JobID is the ID of the unfinished rclone job started on the host.
type jobUnit struct {
	Host   string
	Shards uint
	JobID  int64
}

// bundle represents list of SSTables with the same ID.
type bundle []string

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

	// Loop locations
	for _, l := range target.Location {
		w.Logger.Info(ctx, "Looping locations",
			"cluster_id", w.ClusterID,
			"location", l,
		)

		jobs, err := w.initJobUnits(ctx, run, []string{l.DC, localDC}, resumed)
		if err != nil {
			return errors.Wrap(err, "initialize host pool")
		}

		// Loop manifests for the location
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

					tablePr = w.getTableRunProgress(ctx, run)
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

				version, err := w.RecordTableVersion(ctx, w.clusterSession, fm.Keyspace, fm.Table)
				if err != nil {
					indexErr = err
					return
				}

				var (
					srcDir = l.RemotePath(miwc.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version))
					dstDir = uploadTableDir(fm.Keyspace, fm.Table, version)
				)

				err = w.ExecOnDisabledTable(ctx, w.clusterSession, fm.Keyspace, fm.Table, func() error {
					// Every host has its personal goroutine which is responsible
					// for creating and downloading batches.
					return parallel.Run(len(jobs), target.Parallel, func(n int) error {
						for {
							if ctx.Err() != nil {
								return parallel.Abort(ctx.Err())
							}

							var (
								pr    *RestoreRunProgress
								batch []string
							)

							// Check if host has an already running job
							if jobs[n].JobID == 0 {
								if err := w.validateHostDiskSpace(ctx, jobs[n].Host, target.MinFreeDiskSpace); err != nil {
									w.Logger.Error(ctx, "Validation free disk space error",
										"host", jobs[n].Host,
										"error", err,
									)

									return nil
								}

								takenIDs := chooseIDsForBatch(jobs[n].Shards, target.BatchSize, bundleIDPool)
								if takenIDs == nil {
									w.Logger.Info(ctx, "Empty batch",
										"host", jobs[n].Host,
									)

									return nil
								}

								batch = batchFromIDs(bundles, takenIDs)

								w.Logger.Info(ctx, "Created batch",
									"host", jobs[n].Host,
									"keyspace", fm.Keyspace,
									"table", fm.Table,
									"batch", batch,
								)

								// Download batch to host
								jobID, err := w.Client.RcloneCopyPaths(ctx, jobs[n].Host, dstDir, srcDir, batch)
								if err != nil {
									w.Logger.Error(ctx, "Couldn't download batch to upload dir",
										"host", jobs[n].Host,
										"keyspace", fm.Keyspace,
										"table", fm.Table,
										"srcDir", srcDir,
										"dstDir", dstDir,
										"batch", batch,
										"error", err,
									)
									// Return bundle IDs to the pool so that they can be used in different batch
									returnBatchToPool(bundleIDPool, takenIDs)

									return nil
								}

								w.Logger.Info(ctx, "Created rclone job",
									"host", jobs[n].Host,
									"keyspace", fm.Keyspace,
									"table", fm.Table,
									"job_id", jobID,
									"batch", batch,
								)

								pr = &RestoreRunProgress{
									ClusterID:    run.ClusterID,
									TaskID:       run.TaskID,
									RunID:        run.ID,
									ManifestPath: run.ManifestPath,
									KeyspaceName: fm.Keyspace,
									TableName:    fm.Table,
									Host:         jobs[n].Host,
									AgentJobID:   jobID,
									ManifestIP:   miwc.IP,
									SstableID:    takenIDs,
								}
							} else {
								pr = tablePr[jobs[n].JobID]
								// Mark that previously started job is resumed
								jobs[n].JobID = 0

								batch = batchFromIDs(bundles, pr.SstableID)
							}

							if err := w.waitJob(ctx, pr); err != nil {
								// In case of context cancellation restore is interrupted
								if ctx.Err() != nil {
									return parallel.Abort(ctx.Err())
								}

								w.Logger.Error(ctx, "Couldn't wait on rclone job",
									"host", jobs[n].Host,
									"job ID", jobs[n].JobID,
									"error", err,
								)
								// Since failed progress run might already be recorded in database it has to be deleted
								w.DeleteRunProgress(ctx, pr)
								returnBatchToPool(bundleIDPool, pr.SstableID)

								return nil
							}

							if err := w.Client.Restore(ctx, jobs[n].Host, fm.Keyspace, fm.Table, version, batch); err != nil {
								w.DeleteRunProgress(ctx, pr)
								returnBatchToPool(bundleIDPool, pr.SstableID)

								return nil
							}

							w.Logger.Info(ctx, "Restored batch",
								"host", jobs[n].Host,
								"keyspace", fm.Keyspace,
								"table", fm.Table,
								"batch", batch,
							)
						}
					})
				})

				if err != nil {
					indexErr = err
					return
				}

				if len(bundleIDPool) > 0 {
					indexErr = errors.Wrapf(err, "not restored bundles %v", getChanContent(bundleIDPool))
					return
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

// initJobUnits creates slice of jobUnits with hosts living in dc from dcs.
// Datacenters in dcs are ordered by decreasing priority.
// If none of the nodes living in dcs is alive, pool is initialized with all living nodes.
//
// If resumed is set to false, it also initializes curProgress with information
// about all rclone jobs started on the table specified in run.
// All running jobs are located at the beginning of the result slice.
func (w *restoreWorker) initJobUnits(ctx context.Context, run *RestoreRun, dcs []string, resumed bool) ([]jobUnit, error) {
	status, err := w.Client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get client status")
	}

	var liveNodes scyllaclient.NodeStatusInfoSlice

	// Find live nodes in dcs
	for _, dc := range dcs {
		if liveNodes, err = w.Client.GetLiveNodes(ctx, status.Datacenter([]string{dc})); err == nil {
			break
		}

		w.Logger.Info(ctx, "No live nodes found in dc",
			"dc", dc,
		)
	}

	if liveNodes == nil {
		// Find any live nodes
		liveNodes, err = w.Client.GetLiveNodes(ctx, status)
		if err != nil {
			return nil, errors.New("no live nodes in any dc")
		}
	}

	w.Logger.Info(ctx, "Live nodes",
		"nodes", liveNodes.Hosts(),
	)

	var (
		jobs        []jobUnit
		hostsInPool = strset.New()
	)

	// Collect table progress info from previous run
	if !resumed {
		cb := func(pr *RestoreRunProgress) {
			// Ignore progress created from RecordSize
			if pr.AgentJobID == 0 {
				return
			}
			// Place hosts with unfinished jobs at the beginning
			if pr.CompletedAt == nil {
				sh, err := w.Client.ShardCount(ctx, pr.Host)
				if err != nil {
					w.Logger.Info(ctx, "Couldn't get host shard count",
						"host", pr.Host,
						"error", err,
					)
					return
				}

				jobs = append(jobs, jobUnit{
					Host:   pr.Host,
					Shards: sh,
					JobID:  pr.AgentJobID,
				})

				hostsInPool.Add(pr.Host)
			}
		}

		w.ForEachTableProgress(run, cb)
	}

	for _, n := range liveNodes {
		// Place free hosts in the pool
		if !hostsInPool.Has(n.Addr) {
			sh, err := w.Client.ShardCount(ctx, n.Addr)
			if err != nil {
				w.Logger.Info(ctx, "Couldn't get host shard count",
					"host", n.Addr,
					"error", err,
				)
				continue
			}

			jobs = append(jobs, jobUnit{
				Host:   n.Addr,
				Shards: sh,
			})

			hostsInPool.Add(n.Addr)
		}
	}

	w.Logger.Info(ctx, "Created job units",
		"jobs", jobs,
	)

	return jobs, nil
}

func (w *restoreWorker) getTableRunProgress(ctx context.Context, run *RestoreRun) tableRunProgress {
	tablePr := make(tableRunProgress)

	cb := func(pr *RestoreRunProgress) {
		// Don't include progress created in RecordSize
		if pr.AgentJobID != 0 {
			tablePr[pr.AgentJobID] = pr
		}
	}

	w.ForEachTableProgress(run, cb)

	w.Logger.Info(ctx, "Previous Table progress",
		"keyspace", run.KeyspaceName,
		"table", run.TableName,
		"table_progress", tablePr,
	)

	return tablePr
}

// validateHostDiskSpace checks if host has at least minDiskSpace percent of free disk space.
func (w *restoreWorker) validateHostDiskSpace(ctx context.Context, host string, minDiskSpace int) error {
	disk, err := w.diskFreePercent(ctx, hostInfo{IP: host})
	if err != nil {
		return err
	}
	if disk < minDiskSpace {
		return errors.Errorf("Host %s has %d%% free disk space and requires %d%%", host, disk, minDiskSpace)
	}

	return nil
}

// initBundlePool creates pool of SSTable IDs that have yet to be restored.
// (It does not include ones that are currently being restored).
func initBundlePool(curProgress tableRunProgress, bundles map[string]bundle) chan string {
	var (
		pool      = make(chan string, len(bundles))
		processed = strset.New()
	)

	for _, pr := range curProgress {
		processed.Add(pr.SstableID...)
	}

	for id := range bundles {
		if !processed.Has(id) {
			pool <- id
		}
	}

	return pool
}

// chooseIDsForBatch returns slice of IDs of SSTables that the batch consists of.
func chooseIDsForBatch(shards uint, size int, bundleIDs chan string) []string {
	var (
		batchSize = size * int(shards)
		takenIDs  []string
		done      bool
	)

	// Create batch
	for i := 0; i < batchSize; i++ {
		select {
		case id := <-bundleIDs:
			takenIDs = append(takenIDs, id)
		default:
			done = true
		}

		if done {
			break
		}
	}

	return takenIDs
}

func isSystemKeyspace(keyspace string) bool {
	return strings.HasPrefix(keyspace, "system")
}

func sstableID(file string) string {
	return strings.SplitN(file, "-", 3)[1]
}

// groupSSTablesToBundles maps SSTable ID to its bundle.
func groupSSTablesToBundles(sstables []string) map[string]bundle {
	var bundles = make(map[string]bundle)

	for _, f := range sstables {
		id := sstableID(f)
		bundles[id] = append(bundles[id], f)
	}

	return bundles
}

func returnBatchToPool(pool chan string, ids []string) {
	for _, i := range ids {
		pool <- i
	}
}

// batchFromIDs creates batch of SSTables with IDs present in ids.
func batchFromIDs(bundles map[string]bundle, ids []string) []string {
	var batch []string

	for _, id := range ids {
		batch = append(batch, bundles[id]...)
	}

	return batch
}

func getChanContent(c chan string) []string {
	content := make([]string, len(c))

	for s := range c {
		content = append(content, s)
	}

	return content
}

// GetRestoreProgress aggregates progress for the restore run of the task
// and breaks it down by keyspace and table.json.
// If nothing was found scylla-manager.ErrNotFound is returned.
func (s *Service) GetRestoreProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (RestoreProgress, error) {
	w := &restoreWorker{
		worker: worker{
			ClusterID: clusterID,
			TaskID:    taskID,
			RunID:     runID,
		},
		managerSession: s.session,
	}

	return w.GetProgress(ctx)
}
