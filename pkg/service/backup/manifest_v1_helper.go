// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/pathparser"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// manifestV1Helper provides logic for backup operations like listing, deleting.
// V1 means backups taken pre 2.1 release.
type manifestV1Helper struct {
	host     string
	location Location
	client   *scyllaclient.Client
	logger   log.Logger

	paths manifestV1Paths
}

var _ manifestHelper = &manifestV1Helper{}

func newManifestV1Helper(host string, location Location, client *scyllaclient.Client, logger log.Logger) *manifestV1Helper {
	return &manifestV1Helper{
		host:     host,
		location: location,
		client:   client,
		logger:   logger.With("host", host, "manifest_version", "v1"),
	}
}

// ListManifests return list of manifests present under provided location.
// Manifests are being read in order to collect information about backups.
func (h *manifestV1Helper) ListManifests(ctx context.Context, f ListFilter) ([]*remoteManifest, error) {
	h.logger.Info(ctx, "Listing manifests")

	manifestsPaths, err := h.listPaths(ctx, f)
	if err != nil {
		return nil, errors.Wrap(err, "listing manifests")
	}
	h.logger.Debug(ctx, "Found manifests", "manifests", manifestsPaths)

	manifests := make([]*remoteManifest, len(manifestsPaths))
	for i, mp := range manifestsPaths {
		var err error
		manifests[i], err = h.readManifest(ctx, mp)
		if err != nil {
			return nil, errors.Wrapf(err, "reading manifest %s", mp)
		}
	}

	return manifests, nil
}

func (h *manifestV1Helper) listPaths(ctx context.Context, f ListFilter) ([]string, error) {
	// Load filters
	ksf, err := ksfilter.NewFilter(f.Keyspace)
	if err != nil {
		return nil, err
	}

	dirPrune := h.makeLegacyListFilterPruneDirFunc(ksf, f)
	w := walker{
		Host:     h.host,
		Location: h.location,
		Client:   h.client,
		PruneDir: dirPrune,
	}

	baseDir := path.Join("backup", string(metaDirKind))
	if f.ClusterID != uuid.Nil {
		if f.DC != "" {
			if f.NodeID != "" {
				baseDir = h.paths.RemoteMetaNodeDir(f.ClusterID, f.DC, f.NodeID)
			} else {
				baseDir = path.Join(remoteMetaClusterDCDir(f.ClusterID), f.DC)
			}
		} else {
			baseDir = remoteMetaClusterDCDir(f.ClusterID)
		}
	}

	h.logger.Debug(ctx, "Searching dirs", "base", baseDir)
	dirs, err := w.DirsAtLevelN(ctx, baseDir, h.paths.RemoteMetaKeyspaceLevel(baseDir))
	if err != nil {
		return nil, errors.Wrapf(err, "traverse dir %s on host %s", baseDir, h.host)
	}
	h.logger.Debug(ctx, "Traversing dirs", "size", len(dirs), "dirs", dirs)

	var (
		allManifests []string
		mu           sync.Mutex
	)

	// Deduce parallelism level from nr. of shards
	s, err := h.client.ShardCount(ctx, h.host)
	if err != nil {
		return nil, errors.Wrap(err, "get shard count")
	}
	parallelLimit := int(s*5/2 + 1)
	h.logger.Debug(ctx, "Parallel limit", "limit", parallelLimit)

	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		NoModTime: true,
		Recurse:   true,
	}

	err = h.dirsInParallel(dirs, true, func(baseDir string) error {
		files, err := h.client.RcloneListDir(ctx, h.host, h.location.RemotePath(baseDir), opts)
		if err != nil {
			return errors.Wrapf(err, "list dir %s on host %s", baseDir, h.host)
		}

		for _, f := range files {
			// Filter out unwanted items
			p := path.Join(baseDir, f.Path)
			if dirPrune(p) {
				continue
			}
			m := manifestV1{}

			// It's unlikely but the list may contain manifests and all its
			// sibling files, we want to clear everything but the manifests.
			if err := m.ParsePartialPath(p); err != nil {
				h.logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
					"host", h.host,
					"location", h.location,
					"path", p,
				)
				continue
			}
			// Update all manifests
			mu.Lock()
			allManifests = append(allManifests, p)
			mu.Unlock()
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	h.logger.Debug(ctx, "All manifests", "manifests", len(allManifests))

	return allManifests, nil
}

// ctxt is a context key type.
type ctxt byte

const ctxManifestV1DoNotLoadFiles ctxt = iota

func (h *manifestV1Helper) skipLoadingFiles(ctx context.Context) bool {
	_, ok := ctx.Value(ctxManifestV1DoNotLoadFiles).(bool)
	return ok
}

func (h *manifestV1Helper) readManifest(ctx context.Context, p string) (*remoteManifest, error) {
	m := manifestV1{}
	if err := m.ParsePartialPath(p); err != nil {
		return nil, err
	}

	var (
		fileNames []string
		totalSize int64
	)

	if !h.skipLoadingFiles(ctx) {
		content, err := h.client.RcloneCat(ctx, h.host, h.location.RemotePath(p))
		if err != nil {
			return nil, err
		}

		v := struct {
			Files []string `json:"files"`
		}{}

		if err := json.Unmarshal(content, &v); err != nil {
			return nil, err
		}

		m.Files = v.Files

		// Filter files based on manifest
		files, err := h.client.RcloneListDir(ctx, h.host, h.location.RemotePath(m.RemoteSSTableVersionDir()), nil)
		if err != nil {
			return nil, err
		}

		fileNames = make([]string, 0, len(files))
		s := strset.New(h.extractGroupingKeys(m)...)
		for _, f := range files {
			k, err := groupingKey(path.Join("keyspace", m.Keyspace, "table", m.Table, m.Version, f.Path))
			if err != nil {
				h.logger.Debug(ctx, "GroupingKey error", "error", err)
			}
			if s.Has(k) {
				fileNames = append(fileNames, f.Name)
				totalSize += f.Size
			}
		}
	}

	return &remoteManifest{
		CleanPath:   m.CleanPath,
		Location:    h.location,
		ClusterID:   m.ClusterID,
		DC:          m.DC,
		NodeID:      m.NodeID,
		TaskID:      m.TaskID,
		SnapshotTag: m.SnapshotTag,
		Content: manifestContent{
			Version: "v1",
			Index: []filesInfo{
				{
					Keyspace: m.Keyspace,
					Table:    m.Table,
					Version:  m.Version,
					Files:    fileNames,
				},
			},
			Size: totalSize,
		},
	}, nil
}

func (h *manifestV1Helper) DeleteManifest(ctx context.Context, m *remoteManifest) error {
	h.logger.Info(ctx, "Delete manifest", "snapshot_tag", m.SnapshotTag)

	for _, idx := range m.Content.Index {
		tagsDir := h.paths.RemoteTagsDir(m.ClusterID, m.TaskID, m.DC, m.NodeID, idx.Keyspace, idx.Table)
		path := h.location.RemotePath(path.Join(tagsDir, m.SnapshotTag))

		h.logger.Debug(ctx, "Delete dir", "dst", path)
		err := h.client.RcloneDeleteDir(ctx, h.host, path)
		if err != nil {
			return err
		}
	}

	// V1 manifests are copied to V2 format during migration step. Delete V2
	// format manifest too when snapshot tag is expired.
	v2ManifestPath := remoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, m.DC, m.NodeID)
	return h.client.RcloneDeleteFile(ctx, h.host, h.location.RemotePath(v2ManifestPath))
}

func (h *manifestV1Helper) makeLegacyListFilterPruneDirFunc(ksf *ksfilter.Filter, f ListFilter) func(string) bool {
	return func(dir string) bool {
		var m manifestV1

		// Discard invalid paths
		if err := m.ParsePartialPath(dir); err != nil {
			return true
		}
		if pruneClusterID(m.ClusterID, f) || pruneSnapshotTag(m.SnapshotTag, f) ||
			pruneNodeID(m.NodeID, f) || pruneDC(m.DC, f) {
			return true
		}

		// Filter keyspace and table
		if m.Keyspace != "" && m.Table != "" && len(f.Keyspace) > 0 {
			if !ksf.Check(m.Keyspace, m.Table) {
				return true
			}
		}

		return false
	}
}

func (h *manifestV1Helper) extractGroupingKeys(m manifestV1) []string {
	var s []string
	for _, f := range m.Files {
		v := path.Join("keyspace", m.Keyspace, "table", m.Table, m.Version, strings.TrimSuffix(f, manifestFileSuffix))
		s = append(s, v)
	}
	return s
}

const dirsInParallelLimit = 5

func (h *manifestV1Helper) dirsInParallel(dirs []string, abortOnError bool, f func(d string) error) error {
	return parallel.Run(len(dirs), dirsInParallelLimit, func(i int) error {
		if err := errors.Wrapf(f(dirs[i]), "%s", dirs[i]); err != nil {
			if abortOnError {
				return parallel.Abort(err)
			}
			return err
		}
		return nil
	})
}

type manifestV1 struct {
	CleanPath []string

	ClusterID   uuid.UUID
	DC          string
	NodeID      string
	Keyspace    string
	Table       string
	TaskID      uuid.UUID
	SnapshotTag string
	Version     string

	// Location and Files requires loading manifest.
	Location      Location
	Files         []string
	FilesExpanded []string
}

// ParsePartialPath tries extracting properties from remote path to manifest.
// This is a reverse process to calling RemoteManifestFile function.
// It supports path prefixes i.e. paths that may lead to a manifest file,
// in that case no error is returned but only some fields will be set.
func (m *manifestV1) ParsePartialPath(s string) error {
	// Clear values
	*m = manifestV1{}

	// Ignore empty strings
	if s == "" {
		return nil
	}

	// Clean path for usage with strings.Split
	s = strings.TrimPrefix(path.Clean(s), sep)
	// Set partial clean path
	m.CleanPath = strings.Split(s, sep)

	p := pathparser.New(s, sep)

	err := p.Parse(
		pathparser.Static("backup"),
		pathparser.Static("meta"),
		pathparser.Static("cluster"),
		pathparser.ID(&m.ClusterID),
		pathparser.Static("dc"),
		pathparser.String(&m.DC),
		pathparser.Static("node"),
		pathparser.String(&m.NodeID),
		pathparser.Static("keyspace"),
		pathparser.String(&m.Keyspace),
		pathparser.Static("table"),
		pathparser.String(&m.Table),
		pathparser.Static("task"),
		pathparser.ID(&m.TaskID),
		pathparser.Static("tag"),
		func(v string) error {
			if !isSnapshotTag(v) {
				return errors.Errorf("invalid snapshot tag %s", v)
			}
			m.SnapshotTag = v
			return nil
		},
		pathparser.String(&m.Version),
		pathparser.Static(scyllaManifest),
	)
	if err != nil {
		return err
	}

	return nil
}

func (m manifestV1) RemoteManifestFile() string {
	paths := manifestV1Paths{}
	return paths.RemoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, m.DC, m.NodeID, m.Keyspace, m.Table, m.Version)
}

func (m manifestV1) RemoteSSTableVersionDir() string {
	return remoteSSTableVersionDir(m.ClusterID, m.DC, m.NodeID, m.Keyspace, m.Table, m.Version)
}

type manifestV1Paths struct{}

func (p manifestV1Paths) RemoteMetaNodeDir(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		"backup",
		string(metaDirKind),
		"cluster",
		clusterID.String(),
		"dc",
		dc,
		"node",
		nodeID,
	)
}

func (p manifestV1Paths) RemoteMetaBaseDir(clusterID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		p.RemoteMetaNodeDir(clusterID, dc, nodeID),
		"keyspace",
		keyspace,
		"table",
		table,
	)
}

func (p manifestV1Paths) RemoteMetaKeyspaceLevel(baseDir string) int {
	a := len(strings.Split(p.RemoteMetaBaseDir(uuid.Nil, "a", "b", "c", "d"), sep))
	b := len(strings.Split(baseDir, sep))
	return a - b - 2
}

func (p manifestV1Paths) RemoteManifestFile(clusterID, taskID uuid.UUID, snapshotTag, dc, nodeID, keyspace, table, version string) string {
	return path.Join(
		p.RemoteMetaBaseDir(clusterID, dc, nodeID, keyspace, table),
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		version,
		scyllaManifest,
	)
}

func (p manifestV1Paths) RemoteTagsDir(clusterID, taskID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		p.RemoteMetaBaseDir(clusterID, dc, nodeID, keyspace, table),
		"task",
		taskID.String(),
		"tag",
	)
}
