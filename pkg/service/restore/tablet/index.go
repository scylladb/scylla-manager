// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"
	"net/netip"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	slices2 "github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/topology"
	"golang.org/x/sync/errgroup"
)

// Workload represents all backup metadata needed for tablet aware restore purposes.
type Workload map[table.CQLTable]TableMeta

// TableMeta represents metadata needed for restoring table with tablet aware restore.
type TableMeta struct {
	Table       table.CQLTable
	SnapshotTag string
	// Each DC can be backed up to a separate backup location.
	// Because of that, we need to group scylla manifests per DC.
	Datacenters map[string]DcMeta
	FileCnt     int64
	Size        int64
}

// DcMeta represents table's metadata scoped to a single source DC backed up to a single location.
type DcMeta struct {
	DC       string
	Provider backupspec.Provider
	Bucket   string
	// RemoteManifests are relative to bucket root.
	RemoteManifests []string
}

// IndexWorker is a set of tools responsible for gathering data
// needed for restoring tables with tablet aware restore.
type IndexWorker struct {
	logger log.Logger
	// Topology of the restore destination cluster.
	targetTopology topology.ClusterTopology
	// Returns true if keyspace is replicated with tablets in target cluster.
	isTablet func(ks string) bool
	// Flag marking that tablet aware restore workload indexing should be skipped
	// due to it being unsupported or unconfigured on a cluster level.
	skipIndexing bool
}

// NewIndexWorker is a constructor for IndexWorker.
func NewIndexWorker(ctx context.Context, logger log.Logger, client *scyllaclient.Client, clusterSession gocqlx.Session,
	nodeConfig map[netip.Addr]configcache.NodeConfig, locations []backupspec.Location,
) (*IndexWorker, error) {
	// Check nodes support tablet restore API
	for addr, nc := range nodeConfig {
		ok, err := nc.SupportsTabletRestoreAPI()
		if err != nil {
			return nil, errors.Wrapf(err, "check node %s tablet restore API support", addr)
		}
		if !ok {
			logger.Info(ctx, "Node does not support tablet restore API (available from 2026.3)",
				"node", addr, "version", nc.ScyllaVersion)
			return &IndexWorker{skipIndexing: true}, nil
		}
	}
	// Check nodes have object storage endpoint configured
	locProviders := maps.SetFromSlice(slices2.Map(locations, func(l backupspec.Location) backupspec.Provider {
		return l.Provider
	}))
	for addr, nc := range nodeConfig {
		for p := range locProviders {
			_, err := nc.ScyllaObjectStorageEndpoint(p)
			if err != nil {
				logger.Info(ctx, "Node does not have object storage endpoint configured",
					"node", addr, "provider", p, "error", err)
				return &IndexWorker{skipIndexing: true}, nil
			}
		}
	}
	// Build target topology
	sessionIter := topology.BuildSessionIter(ctx, clusterSession.Session, false)
	targetTopo := topology.BuildClusterTopology(sessionIter.Iter)
	if sessionIter.Err != nil {
		return nil, errors.Wrap(sessionIter.Err, "build target cluster topology")
	}
	// Build ks replication checker
	rd := scyllaclient.NewRingDescriber(ctx, client)
	return &IndexWorker{
		logger:         logger.Named("tablet_index"),
		targetTopology: targetTopo,
		isTablet:       rd.IsTabletKeyspace,
	}, nil
}

// Index gathers data needed for restoring given tables with tablet aware restore
// based on provided manifests and their indexes. Returned map contains entries
// only for the tables eligible for tablet aware restore.
func (w *IndexWorker) Index(ctx context.Context, tables map[table.CQLTable]struct{}, manifests []backupspec.ManifestInfoWithContent) (Workload, error) {
	w.logger.Info(ctx, "Started indexing tablet aware restore workload")
	defer w.logger.Info(ctx, "Finished indexing tablet aware restore workload")

	workload := make(Workload)
	if w.skipIndexing {
		return workload, nil
	}
	if ok, err := w.checkManifestsCompatibility(ctx, manifests); err != nil {
		return nil, errors.Wrap(err, "check manifests compatibility")
	} else if !ok {
		return workload, nil
	}

	// Initially include all given tables as tablet restore compatible
	// and remove them from this map as they fail the checks.
	for t := range tables {
		workload[t] = TableMeta{
			Table:       t,
			Datacenters: make(map[string]DcMeta),
		}
	}
	wMu := sync.RWMutex{}

	// Since iterating over manifest index requires
	// reading from files, do it in parallel.
	eg := errgroup.Group{}
	eg.SetLimit(backup.MaxManifestInMemory)
	for _, m := range manifests {
		eg.Go(func() error {
			err := m.ForEachIndexIterWithError(nil, func(fm backupspec.FilesMeta) error {
				t := table.CQLTable{Keyspace: fm.Keyspace, Name: fm.Table}
				// No need to check files meta compatibility for
				// tables that already failed compatibility check.
				wMu.RLock()
				_, ok := workload[t]
				wMu.RUnlock()
				if !ok {
					return nil
				}
				// As files meta compatibility check requires parsing
				// all file names, do it without holding the mutex.
				ok, err := w.checkFilesMetaCompatibility(ctx, fm)
				wMu.Lock()
				defer wMu.Unlock()

				switch {
				case err != nil:
					return errors.Wrapf(err, "check table %s.%s files meta compatibility", fm.Keyspace, fm.Table)
				case !ok:
					delete(workload, t)
					return nil
				default:
					// As we don't check files meta compatibility under mutex,
					// currently analyzed table might have failed compatibility
					// check in the meantime.
					tm, ok := workload[t]
					if !ok {
						return nil
					}
					manifestPath := m.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
					remoteScyllaManifests := slices2.Map(fm.ScyllaManifests, func(sm string) string {
						return path.Join(manifestPath, sm)
					})
					// Update corresponding source DC metadata
					dcm := tm.Datacenters[m.DC]
					dcm.DC = m.DC
					dcm.Provider = m.Location.Provider
					dcm.Bucket = m.Location.Path
					dcm.RemoteManifests = append(dcm.RemoteManifests, remoteScyllaManifests...)
					tm.Datacenters[dcm.DC] = dcm
					// Update corresponding table metadata
					tm.SnapshotTag = m.SnapshotTag
					tm.Size += fm.Size
					tm.FileCnt += int64(len(fm.Files))
					workload[t] = tm
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
	for t, tm := range workload {
		if len(tm.Datacenters) == 0 {
			delete(workload, t)
			continue
		}
		// Scylla API limitation that will be removed in the future
		if len(tm.Datacenters) != 1 {
			w.logger.Info(ctx, "Tablet-aware restore is supported only when table's backup contains a single DC")
			delete(workload, t)
		}
	}
	return workload, nil
}

// Even though tablet restore API is available from a later version,
// scylla manifests that can be used for tablet aware restore purposes
// are available from an earlier version.
const scyllaTabletAwareBackupSupport = ">= 2026.1.1"

// checkManifestsCompatibility checks backup-level compatibility with tablet restore.
func (w *IndexWorker) checkManifestsCompatibility(ctx context.Context, manifests []backupspec.ManifestInfoWithContent) (bool, error) {
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
	if !w.targetTopology.ContainsDCsAndRacks(backupTopo) {
		w.logger.Info(ctx, "Backup cluster topology contains data centers or racks not present in target cluster topology "+
			"(they can be excluded from restore procedure with --dc-mapping)",
			"backup topology", backupTopo, "target topology", w.targetTopology)
		return false, nil
	}

	// Scylla limitation that will be removed in the future
	if len(w.targetTopology.DCs) != 1 {
		w.logger.Info(ctx, "Tablet-aware restore is supported only when restoring into a single DC cluster")
		return false, nil
	}

	return true, nil
}

func (w *IndexWorker) checkFilesMetaCompatibility(ctx context.Context, fm backupspec.FilesMeta) (bool, error) {
	// Check scylla manifests presence
	if len(fm.ScyllaManifests) == 0 {
		w.logger.Info(ctx, "No scylla manifests found",
			"keyspace", fm.Keyspace, "table", fm.Table)
		return false, nil
	}

	// Check replication type
	if scyllaclient.KeyspaceReplication(fm.ReplicationType) != scyllaclient.ReplicationTablet {
		w.logger.Info(ctx, "Backed up keyspace is not replicated with tablets",
			"keyspace", fm.Keyspace, "replication type", fm.ReplicationType)
		return false, nil
	}
	if !w.isTablet(fm.Keyspace) {
		w.logger.Info(ctx, "Keyspace in restore target cluster is not replicated with tablets",
			"keyspace", fm.Keyspace)
		return false, nil
	}

	// Check sstable ID type
	for _, f := range fm.Files {
		id, err := sstable.ParseID(f)
		if err != nil {
			return false, errors.Wrap(err, "parse sstable ID")
		}
		if id.Type == sstable.IntegerID {
			w.logger.Info(ctx, "SSTable with integer based ID",
				"keyspace", fm.Keyspace, "table", fm.Table, "file", f)
			return false, nil
		}
	}

	return true, nil
}
