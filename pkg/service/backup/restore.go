package backup

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TODO docstrings

type RestoreTarget struct {
	Location         []Location `json:"location"`
	SnapshotTag      string     `json:"snapshot_tag"`
	Keyspace         []string   `json:"keyspace"`
	Table            []string   `json:"table"`
	BatchSize        int        `json:"batch_size"`
	MinFreeDiskSpace int        `json:"min_free_disk_space"`
	Continue         bool       `json:"continue"`
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

	for _, l := range target.Location {
		s.logger.Info(ctx, "Looping locations",
			"cluster_id", clusterID,
			"location", l,
		)

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

		// ~TODO~ - Wait for schema agreement
		// Does this actually need to be done per-location or just once?
		if err := func() error {
			w.Logger.Info(ctx, "Awaiting Schema Agreement")
			clusterSession, err := s.clusterSession(ctx, clusterID)
			if err != nil {
				// TODO - backup ignores this error, but here we probably can't?
				w.Logger.Info(ctx, "No CQL cluster session, backup of schema as CQL files would be skipped", "error", err)
				return nil
			}
			defer clusterSession.Close()

			w.AwaitSchemaAgreement(ctx, clusterSession)
			return nil
		}(); err != nil {
			return err
		}

		// TODO - Find live nodes local to the backup data - same DC
		// var liveNodes scyllaclient.NodeStatusInfoSlice
		// If cannot be found use Scylla nodes in local DC

		// Loop manifests for the snapshot tag
		s.forEachManifest(ctx, clusterID, []Location{l}, ListFilter{SnapshotTag: target.SnapshotTag}, func(miwc ManifestInfoWithContent) {
			s.logger.Info(ctx, "Looping manifests",
				"cluster_id", clusterID,
				"location", l,
				"manifest", miwc.ManifestInfo,
			)

			// TODO - replace with streaming indexes when #3171 merged
			// TODO - filter for requests tables - restoring all for now
			for _, i := range miwc.Index {
				s.logger.Info(ctx, "Looping tables",
					"cluster_id", clusterID,
					"location", l,
					"table", i.Table,
				)

				// TODO - disable compaction on all nodes
				// TODO - defer enable compaction on all nodes

				/* TODO - Get/Set/Reset grace seconds
				graceSeconds, err := w.RecordGraceSeconds(ctx, clusterSession, i.Keyspace, i.Table)
				if err != nil {
					s.logger.Error(ctx, "error recording gc_grace_seconds", "err", err)
					return
				}
				s.logger.Info(ctx, "recorded gc_grace_seconds", "graceSeconds", graceSeconds)
				// TODO - what is max?
				w.SetGraceSeconds(ctx, clusterSession, i.Keyspace, i.Table, 999999999)
				// TODO - make this all in a function per-table so deferred func happens immediately after each table
				defer w.SetGraceSeconds(ctx, clusterSession, i.Keyspace, i.Table, graceSeconds)
				*/

				dir := RemoteSSTableVersionDir(miwc.ClusterID, miwc.DC, miwc.NodeID, i.Keyspace, i.Table, i.Version)
				for _, f := range i.Files {
					s.logger.Info(ctx, "Restoring File",
						"dir", dir,
						"file", f,
					)
				}

				client.Restore(ctx, miwc.Location.String(), dir, i.Keyspace, i.Table, i.Version, i.Files)

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
		})
	}

	return nil
}
