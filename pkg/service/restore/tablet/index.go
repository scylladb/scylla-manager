// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"
	"net/netip"
	"path"
	"runtime"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
	slices2 "github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/topology"
	"golang.org/x/sync/errgroup"
)

// IndexWorker is a set of tools responsible for gathering data
// needed for restoring tables with tablet aware restore.
type IndexWorker struct {
	logger         log.Logger
	client         *scyllaclient.Client
	clusterSession gocqlx.Session
	nodeConfig     map[netip.Addr]configcache.NodeConfig
	dcMapping      map[string]string
}

// NewIndexWorker is a constructor for IndexWorker.
func NewIndexWorker(logger log.Logger, client *scyllaclient.Client, clusterSession gocqlx.Session,
	nodeConfig map[netip.Addr]configcache.NodeConfig, dcMapping map[string]string,
) *IndexWorker {
	return &IndexWorker{
		logger:         logger.Named("tablet_index"),
		client:         client,
		clusterSession: clusterSession,
		nodeConfig:     nodeConfig,
		dcMapping:      dcMapping,
	}
}

// Index gathers data needed for restoring given tables with tablet aware restore
// based on provided manifests and their indexes. Returned map contains entries
// only for the tables eligible for tablet aware restore.
func (w *IndexWorker) Index(ctx context.Context, tables map[table.CQLTable]struct{}, manifests []backupspec.ManifestInfoWithContent) (Workload, error) {
	w.logger.Info(ctx, "Started indexing tablet aware restore workload")
	defer w.logger.Info(ctx, "Finished indexing tablet aware restore workload")

	if ok, err := w.checkManifestsCompatibility(ctx, manifests); err != nil {
		return nil, errors.Wrap(err, "check manifests compatibility")
	} else if !ok {
		return nil, nil // nolint: nilnil
	}

	// Initially include all given tables as tablet restore compatible
	// and remove them from this map as they fail the checks.
	tabletTables := make(Workload)
	for t := range tables {
		tabletTables[t] = TableMeta{
			Table:           t,
			RemoteManifests: make(map[backupspec.Location][]string),
		}
	}
	ttMu := sync.RWMutex{}

	// Since iterating over manifest index requires
	// reading from files, do it in parallel.
	eg := errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())
	for _, m := range manifests {
		eg.Go(func() error {
			err := m.ForEachIndexIterWithError(nil, func(fm backupspec.FilesMeta) error {
				t := table.CQLTable{Keyspace: fm.Keyspace, Name: fm.Table}
				// No need to check files meta compatibility for
				// tables that already failed compatibility check.
				ttMu.RLock()
				_, ok := tabletTables[t]
				ttMu.RUnlock()
				if !ok {
					return nil
				}

				// As files meta compatibility check requires parsing
				// all file names, do it without holding the mutex.
				ok, err := w.checkFilesMetaCompatibility(ctx, fm)
				ttMu.Lock()
				defer ttMu.Unlock()
				switch {
				case err != nil:
					return errors.Wrapf(err, "check table %s.%s files meta compatibility", fm.Keyspace, fm.Table)
				case !ok:
					delete(tabletTables, t)
					return nil
				default:
					// As we don't check files meta compatibility under mutex,
					// currently analyzed table might have failed compatibility
					// check in the meantime.
					tm, ok := tabletTables[t]
					if !ok {
						return nil
					}
					manifestPath := m.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
					remoteScyllaManifests := slices2.Map(fm.ScyllaManifests, func(sm string) string {
						return path.Join(manifestPath, sm)
					})
					tm.SnapshotTag = m.SnapshotTag

					// Construct location instead taking the one from the manifest.
					// The location saved in the manifest and location used during restore
					// might differ, as backup files might have been moved after backup was made.
					// To fix that, we would need to overwrite Provider and Path here with the ones
					// taken from the Location used to list this manifest. This is a common problem
					// for other restore types as well. Here, we at least correctly handle the DC
					// part and apply dc mapping on top of it.
					location := backupspec.Location{
						DC:       m.DC,
						Provider: m.Location.Provider,
						Path:     m.Location.Path,
					}
					if len(w.dcMapping) > 0 {
						// Manifests not present in non-zero dc mapping were already filtered out
						location.DC = w.dcMapping[location.DC]
					}
					tm.RemoteManifests[location] = append(tm.RemoteManifests[location], remoteScyllaManifests...)
					tm.Size += fm.Size
					tm.FileCnt += int64(len(fm.Files))
					tabletTables[t] = tm
					return nil
				}
			})
			return errors.Wrapf(err, "iterate over node %s manifest index", m.NodeID)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Cleanup not encountered tables
	for t, tm := range tabletTables {
		if len(tm.RemoteManifests) == 0 {
			delete(tabletTables, t)
		}
	}
	return tabletTables, nil
}

// Even though tablet restore API is available from a later version,
// scylla manifests that can be used for tablet aware restore purposes
// are available from an earlier version.
const scyllaTabletAwareBackupSupport = ">= 2026.1.1"

// checkManifestsCompatibility checks backup-level compatibility with tablet restore.
func (w *IndexWorker) checkManifestsCompatibility(ctx context.Context, manifests []backupspec.ManifestInfoWithContent) (bool, error) {
	// Check nodes support tablet restore API
	for addr, nc := range w.nodeConfig {
		ok, err := nc.SupportsTabletRestoreAPI()
		if err != nil {
			return false, errors.Wrapf(err, "check node %s tablet restore API support", addr)
		}
		if !ok {
			w.logger.Info(ctx, "Node does not support tablet restore API (available from 2026.2)", "node", addr, "version", nc.ScyllaVersion)
			return false, nil
		}
	}

	// Check scylla manifests support tablet restore
	for _, m := range manifests {
		ok, err := version.CheckConstraint(m.ScyllaVersion, scyllaTabletAwareBackupSupport)
		if err != nil {
			return false, errors.Wrapf(err, "check node %s backed up scylla version", m.NodeID)
		}
		if !ok {
			w.logger.Info(ctx, "Backed up scylla manifests do not support tablet restore (available from 2026.1.1)", "node", m.NodeID, "version", m.ScyllaVersion)
			return false, nil
		}
	}

	// Check backup and target topologies have the same dcs and racks
	backupIter := func(yield func(string, string) bool) {
		for _, m := range manifests {
			if !yield(m.DC, m.Rack) {
				return
			}
		}
	}
	backupTopo := topology.BuildClusterTopology(backupIter)

	sessionIter := topology.BuildSessionIter(ctx, w.clusterSession.Session, false)
	targetTopo := topology.BuildClusterTopology(sessionIter.Iter)
	if sessionIter.Err != nil {
		return false, errors.Wrap(sessionIter.Err, "build target cluster topology")
	}

	if !targetTopo.ContainsDCsAndRacks(backupTopo) {
		w.logger.Info(ctx, "Backup cluster topology contains data centers or racks not present in target cluster topology",
			"backup topology", backupTopo, "target topology", targetTopo)
		return false, nil
	}

	return true, nil
}

func (w *IndexWorker) checkFilesMetaCompatibility(ctx context.Context, fm backupspec.FilesMeta) (bool, error) {
	// Check scylla manifests presence
	if len(fm.ScyllaManifests) == 0 {
		w.logger.Info(ctx, "No scylla manifests found", "keyspace", fm.Keyspace, "table", fm.Table)
		return false, nil
	}

	// Check replication type
	if scyllaclient.KeyspaceReplication(fm.ReplicationType) != scyllaclient.ReplicationTablet {
		w.logger.Info(ctx, "Keyspace not replicated with tablets", "keyspace", fm.Keyspace, "table", fm.Table, "replication type", fm.ReplicationType)
		return false, nil
	}

	// Check sstable ID type
	for _, f := range fm.Files {
		id, err := sstable.ParseID(f)
		if err != nil {
			return false, errors.Wrap(err, "parse sstable ID")
		}
		if id.Type == sstable.IntegerID {
			w.logger.Info(ctx, "SSTable with integer based ID", "keyspace", fm.Keyspace, "table", fm.Table, "file", f)
			return false, nil
		}
	}

	return true, nil
}
