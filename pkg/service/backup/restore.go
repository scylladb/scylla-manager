package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/pkg/errors"
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
	// TODO - gather livenodes here like in backup?

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

	hosts := make([]hostInfo, len(target.Location))
	for i := range target.Location {
		hosts[i].Location = target.Location[i]
	}
	if err := s.resolveHosts(ctx, client, hosts); err != nil {
		return errors.Wrap(err, "resolve hosts")
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	// TODO: can we have one worker for all locations?
	// TODO - create distinct restore worker?
	w := &worker{
		ClusterID:   clusterID,
		ClusterName: clusterName,
		TaskID:      taskID,
		RunID:       runID,
		Client:      client,
		Config:      s.config,
		Metrics:     s.metrics,
	}

	// Get cluster session
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		// TODO - backup ignores this error, but here we probably can't?
		w.Logger.Info(ctx, "No CQL cluster session, restore can't proceed", "error", err)
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

		// TODO: add stages
		// ~TODO~ - Wait for schema agreement
		// Does this actually need to be done per-location or just once?

		w.Logger.Info(ctx, "Awaiting Schema Agreement")

		w.AwaitSchemaAgreement(ctx, clusterSession)

		// Get nodes from the backup location DC
		liveNodes, err := client.GetLiveNodes(ctx, status, []string{l.DC})
		if err != nil {
			// In case of failure get nodes from local DC
			liveNodes, err = client.GetLiveNodes(ctx, status, []string{s.config.LocalDC})
			if err != nil {
				return err
			}
		}

		w.Logger.Info(ctx, "Live nodes",
			"nodes", liveNodes,
		)

		// TODO: do we need to validate if liveNodes can restore backup?
		// If yes then can we do it now or do we need to iterate over all manifests to accomplish that?

		// Loop manifests for the snapshot tag
		s.forEachManifest(ctx, clusterID, []Location{l}, ListFilter{SnapshotTag: target.SnapshotTag}, func(miwc ManifestInfoWithContent) error {
			s.logger.Info(ctx, "Looping manifests",
				"cluster_id", clusterID,
				"location", l,
				"manifest", miwc.ManifestInfo,
			)

			// Filter keyspaces
			filter, err := ksfilter.NewFilter(target.Keyspace)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("crete filter for restored tables in location %s", l.String()))
			}

			for _, i := range miwc.Index {
				filter.Add(i.Keyspace, []string{i.Table})
			}
			// Get the filtered units
			units, _ := filter.Apply(false)

			// Loop tables for the manifest
			for _, u := range units {
				for _, t := range u.Tables {
					s.logger.Info(ctx, "Looping tables",
						"cluster_id", clusterID,
						"location", l,
						"keyspace", u.Keyspace,
						"table", t,
					)

					if err := func() error {
						// Temporarily disable compaction
						comp, err := w.RecordCompaction(ctx, clusterSession, u.Keyspace, t)
						if err != nil {
							return err
						}

						s.logger.Info(ctx, "recorded compaction",
							"compaction", comp.String(),
						)

						var tmpComp compaction
						for k, v := range comp {
							tmpComp[k] = v
						}
						// Disable compaction option
						tmpComp["enabled"] = "false"

						if err := w.SetCompaction(ctx, clusterSession, u.Keyspace, t, tmpComp); err != nil {
							return err
						}
						// TODO: what if resetting compaction/gc_grace_seconds fails?
						// Reset compaction
						defer w.SetCompactionNoErr(ctx, clusterSession, u.Keyspace, t, comp)

						// Temporarily set gc_grace_seconds to max supported value
						ggs, err := w.RecordGraceSeconds(ctx, clusterSession, u.Keyspace, t)

						s.logger.Info(ctx, "recorded gc_grace_seconds",
							"gc_grace_seconds", ggs,
						)

						const maxGGS = math.MaxInt32
						if err := w.SetGraceSeconds(ctx, clusterSession, u.Keyspace, t, maxGGS); err != nil {
							return err
						}
						// Reset gc_grace_seconds
						defer w.SetGraceSecondsNoErr(ctx, clusterSession, u.Keyspace, t, ggs)

						dir := RemoteSSTableVersionDir(miwc.ClusterID, miwc.DC, miwc.NodeID, u.Keyspace, t, i.Version)

						client.Restore(ctx, miwc.Location.String(), dir, i.Keyspace, i.Table, i.Version, i.Files)

						return nil
					}(); err != nil {
						return errors.Wrap(err, fmt.Sprintf("restore table %s.%s", u.Keyspace, t))
					}
				}
			}

			// TODO - replace with streaming indexes when #3171 merged
			for _, i := range miwc.Index {

				for _, f := range i.Files {
					s.logger.Info(ctx, "Restoring File",
						"dir", dir,
						"file", f,
					)
				}

				// TODO: are file names actually sstable ID? table_name-UUID format?

				/* TODO - BUNDLE AND BATCH - starting 1 at a time for simplicity
				// TODO - Group related files to bundles by sstable ID
				// TODO Join bundles into batches
				var batches []string
				for _, b := range batches {
					s.logger.Info(ctx, "Looping batches",
						"cluster_id", clusterID,
						"location", l,
						// "manifest", miwc,
						"table", i.Table,
						"batch", b,
					)
					// TODO - check disk space
					// TODO - download bundle to upload dir
					// Wait for completion
				}
				*/
			}
			return nil
		})
	}

	return nil
}
