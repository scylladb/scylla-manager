package backup

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TODO docstrings

type RestoreTarget struct {
	Location    []Location `json:"location"`
	SnapshotTag string     `json:"snapshot_tag"`
	// TODO: should we replace Keyspace + Table with Unit and set them in GetRestoreTarget?
	Keyspace         []string `json:"keyspace"`
	Table            []string `json:"table"`
	BatchSize        int      `json:"batch_size"`
	MinFreeDiskSpace int      `json:"min_free_disk_space"`
	Continue         bool     `json:"continue"`
}

type RestoreRunner struct {
	service *Service
}

func (r RestoreRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	t, err := r.service.GetRestoreTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get restore target")
	}
	return r.service.Restore(ctx, clusterID, taskID, runID, t)
}

func (s *Service) RestoreRunner() RestoreRunner {
	return RestoreRunner{service: s}
}

func (s *Service) GetRestoreTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (RestoreTarget, error) {
	s.logger.Info(ctx, "GetRestoreTarget", "cluster_id", clusterID)

	var t RestoreTarget
	// TODO: in backup we were unmarshalling to taskProperties. Why is it different here?
	if err := json.Unmarshal(properties, &t); err != nil {
		return t, err
	}

	if t.Location == nil {
		return t, errors.New("missing location")
	}

	if t.BatchSize == 0 {
		// In case of 0 set to default value
		t.BatchSize = 2
	}

	return t, nil
}

func (s *Service) Restore(ctx context.Context, clusterID, taskID, runID uuid.UUID, target RestoreTarget) error {
	s.logger.Info(ctx, "Restore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	// TODO create progress tracking struct

	if target.Continue {
		panic("TODO - implement resume")
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "initialize: get client proxy")
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	w := &worker{
		ClusterID:     clusterID,
		ClusterName:   clusterName,
		TaskID:        taskID,
		RunID:         runID,
		Client:        client,
		Config:        s.config,
		Metrics:       s.metrics,
		Logger:        s.logger,
		OnRunProgress: s.insertWithLogError,
	}

	// Get cluster session
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		s.logger.Info(ctx, "No CQL cluster session, restore can't proceed", "error", err)
		return err
	}
	defer clusterSession.Close()

	// Get hosts in all DCs
	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get result")
	}

	for _, l := range target.Location {
		s.logger.Info(ctx, "Looping locations",
			"cluster_id", clusterID,
			"location", l,
		)

		// TODO: Does this actually need to be done per-location or just once?

		s.logger.Info(ctx, "Awaiting Schema Agreement")

		w.AwaitSchemaAgreement(ctx, clusterSession)

		// Get live nodes from the backup location DC
		liveNodes, err := client.GetLiveNodes(ctx, status, []string{l.DC})
		if err != nil {
			// In case of failure get live nodes from local DC
			liveNodes, err = client.GetLiveNodes(ctx, status, []string{s.config.LocalDC})
			if err != nil {
				return errors.Errorf("no live nodes found in dc: %v", []string{l.DC, s.config.LocalDC})
			}
		}

		s.logger.Info(ctx, "Live nodes",
			"nodes", liveNodes,
		)

		// Initialize host pool
		hostPool := make(chan string, len(liveNodes))
		for _, n := range liveNodes {
			hostPool <- n.Addr
		}

		// Filter keyspaces
		filter, err := ksfilter.NewFilter(target.Keyspace)
		if err != nil {
			return errors.Wrap(err, "crete filter for restored tables in location")
		}

		lf := ListFilter{SnapshotTag: target.SnapshotTag}

		// Loop manifests for the snapshot tag
		err = s.forEachManifest(ctx, clusterID, []Location{l}, lf, func(miwc ManifestInfoWithContent) error {
			s.logger.Info(ctx, "Looping manifests",
				"cluster_id", clusterID,
				"location", l,
				"manifest", miwc.ManifestInfo,
			)

			// Loop tables for the manifest
			return miwc.ForEachIndexIter(func(fm FilesMeta) {
				// Skip system and filtered out tables
				if isSystemKeyspace(fm.Keyspace) || !filter.Check(fm.Keyspace, fm.Table) || len(fm.Files) == 0 {
					return
				}

				s.logger.Info(ctx, "Looping tables",
					"cluster_id", clusterID,
					"location", l,
					"keyspace", fm.Keyspace,
					"table", fm.Table,
				)

				// group files to SSTable bundles
				bundles := groupSSTablesByID(fm.Files)

				// Initialize bundle index pool
				bundlePool := make(chan int, len(bundles))
				for i := range bundles {
					bundlePool <- i
				}

				srcDir := l.RemotePath(miwc.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version))

				version, err := w.RecordTableVersion(ctx, clusterSession, fm.Keyspace, fm.Table)
				if err != nil {
					return
				}
				version = strings.ReplaceAll(version, "-", "")

				err = w.ExecOnDisabledTable(ctx, clusterSession, fm.Keyspace, fm.Table, func() error {
					// TODO: change it to work in parallel
					for {
						// Get host from the pool
						host := ""
						select {
						case host = <-hostPool:
						default:
						}

						if host == "" {
							s.logger.Info(ctx, "No more hosts in the pool, restore can't proceed")

							break
						}

						if err := w.validateHostDiskSpace(ctx, host, target.MinFreeDiskSpace); err != nil {
							s.logger.Info(ctx, "Couldn't validate host's free disk space",
								"host", host,
								"error", err,
							)
							// TODO: what to do with this host? Do we want to put him back in the pool?
							continue
						}

						shards, err := client.ShardCount(ctx, host)
						if err != nil {
							s.logger.Error(ctx, "Couldn't get host shard count",
								"host", host,
							)
							// TODO: what to do with this host? Do we want to put him back in the pool?
							continue
						}

						batchSize := target.BatchSize * int(shards)
						var (
							batch          []string
							takenIdx       []int
							takenSSTableID []string
							done           bool
						)

						// Create batch
						for i := 0; i < batchSize; i++ {
							select {
							case idx := <-bundlePool:
								batch = append(batch, bundles[idx]...)
								takenIdx = append(takenIdx, idx)
								takenSSTableID = append(takenSSTableID, sstableID(bundles[idx][0]))
							default:
								done = true
							}

							if done {
								break
							}
						}

						if len(batch) == 0 {
							break
						}

						s.logger.Info(ctx, "Looping batches",
							"cluster_id", clusterID,
							"location", l,
							"keyspace", fm.Keyspace,
							"table", fm.Table,
							"host", host,
							"batch", batch,
						)

						dstDir := uploadTableDir(fm.Keyspace, fm.Table, version)

						jobID, err := client.RcloneCopyPaths(ctx, host, dstDir, srcDir, batch)
						if err != nil {
							s.logger.Error(ctx, "Couldn't download files to host's upload dir",
								"host", host,
								"srcDir", srcDir,
								"dstDir", dstDir,
								"files", batch,
							)

							returnBundleIdx(bundlePool, takenIdx)

							continue
						}

						//TODO: record progress
						pr := &RestoreRunProgress{
							ClusterID:    clusterID,
							TaskID:       taskID,
							RunID:        runID,
							NodeID:       miwc.NodeID,
							KeyspaceName: fm.Keyspace,
							TableName:    fm.Table,
							Host:         host,
							AgentJobID:   jobID,
							SstableIdx:   takenSSTableID,
						}

						if err := w.waitRestoreJob(ctx, pr); err != nil {
							returnBundleIdx(bundlePool, takenIdx)

							continue
						}

						if err := client.Restore(ctx, host, fm.Keyspace, fm.Table, version, batch); err != nil {
							returnBundleIdx(bundlePool, takenIdx)

							continue
						}

						// return host to the pool
						hostPool <- host

						// end work if there are no more bundles to process
						if done {
							break
						}
					}

					var (
						failed []string
						done   bool
					)

					for {
						select {
						case idx := <-bundlePool:
							failed = append(failed, bundles[idx]...)
						default:
							done = true
						}

						if done {
							break
						}
					}

					if len(failed) > 0 {
						return errors.Errorf("couldn't restore following files: %v", failed)
					}

					return nil
				})

				if err != nil {
					s.logger.Error(ctx, "restoring table failed",
						"keyspace", fm.Keyspace,
						"table", fm.Table,
						"error", err,
					)
				}
			})
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func isSystemKeyspace(keyspace string) bool {
	return strings.HasPrefix(keyspace, "system")
}

func sstableID(file string) string {
	return strings.SplitN(file, "-", 3)[1]
}

func groupSSTablesByID(files []string) [][]string {
	var bundles [][]string
	// maps SSTable ID to its bundle index
	idIndex := make(map[string]int)

	for _, f := range files {
		id := sstableID(f)
		if idx, ok := idIndex[id]; !ok {
			idx = len(bundles)
			bundles = append(bundles, nil)
			bundles[idx] = append(bundles[idx], f)
			idIndex[id] = idx
		} else {
			bundles[idx] = append(bundles[idx], f)
		}
	}

	return bundles
}

// validateHostDiskSpace checks if host has at least minDiskSpace percent of free disk space.
func (w *worker) validateHostDiskSpace(ctx context.Context, host string, minDiskSpace int) error {
	disk, err := w.diskFreePercent(ctx, hostInfo{IP: host})
	if err != nil {
		return err
	}
	if disk < minDiskSpace {
		return errors.Errorf("Host %s has %d%% free disk space and requires %d%%", host, disk, minDiskSpace)
	}

	return nil
}

func returnBundleIdx(pool chan int, idx []int) {
	for _, i := range idx {
		pool <- i
	}
}

// RecordRestoreSize records size of every table from every manifest.
// Resuming is implemented on manifest (nodeID) level.
func (s *Service) RecordRestoreSize(ctx context.Context, run RestoreRun, target RestoreTarget) error {
	var resumed bool
	if !target.Continue || run.NodeID == "" {
		resumed = true
	}

	pr := &RestoreRunProgress{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		RunID:     run.ID,
	}

	for _, l := range target.Location {
		lf := ListFilter{SnapshotTag: target.SnapshotTag}

		err := s.forEachManifest(ctx, run.ClusterID, []Location{l}, lf, func(miwc ManifestInfoWithContent) error {
			if !resumed {
				if run.NodeID != miwc.NodeID {
					return nil
				}
				resumed = true
			}

			pr.NodeID = miwc.NodeID
			// Set IP of the manifest node
			pr.Host = miwc.IP

			err := miwc.ForEachIndexIter(func(fm FilesMeta) {
				pr.KeyspaceName = fm.Keyspace
				pr.TableName = fm.Table
				pr.Size = fm.Size
				// Record progress for table
				s.insertWithLogError(ctx, pr)
			})
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// decorateWithPrevRestoreRun gets restore task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRestoreRun(ctx context.Context, run *RestoreRun) error {
	prev, err := s.GetLastResumableRestoreRun(ctx, run.ClusterID, run.TaskID)
	if errors.Is(err, service.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get previous restore run")
	}

	// TODO: do we have to validate the time of previous run?

	s.logger.Info(ctx, "Resuming previous restore run", "prev_run_id", prev.ID)

	run.PrevID = prev.ID
	run.NodeID = prev.NodeID
	run.Keyspace = prev.Keyspace
	run.Table = prev.Table
	run.Stage = prev.Stage

	return nil
}

func (s *Service) clonePrevRestoreProgress(run *RestoreRun) error {
	q := table.RestoreRunProgress.InsertQuery(s.session)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	return s.ForEachRestoreProgressIter(bindForAll(prevRun), func(pr *RestoreRunProgress) error {
		pr.RunID = run.ID
		return q.BindStruct(pr).Exec()
	})
}

// GetLastResumableRestoreRun returns the most recent started but not done run of
// the restore task, if there is a recent run that is completely done ErrNotFound is reported.
func (s *Service) GetLastResumableRestoreRun(ctx context.Context, clusterID, taskID uuid.UUID) (*RestoreRun, error) {
	s.logger.Debug(ctx, "GetLastResumableRestoreRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	q := qb.Select(table.RestoreRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(1).Query(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*RestoreRun
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	for _, r := range runs {
		if r.Stage == StageRestoreDone {
			break
		} else {
			return r, nil
		}
	}

	return nil, service.ErrNotFound
}
