// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// manifestV2Helper allows to list and read all manifests in given Location and filter
// them using provided ListFilter.
type manifestV2Helper struct {
	host     string
	location Location
	client   *scyllaclient.Client
	logger   log.Logger
}

var _ manifestHelper = &manifestV2Helper{}

func newManifestV2Helper(host string, location Location, client *scyllaclient.Client, logger log.Logger) *manifestV2Helper {
	return &manifestV2Helper{
		host:     host,
		location: location,
		client:   client,
		logger:   logger.With("host", host, "manifest_version", "v2"),
	}
}

func (h *manifestV2Helper) ListManifests(ctx context.Context, f ListFilter) ([]*remoteManifest, error) {
	h.logger.Info(ctx, "Listing manifests")

	manifestsPaths, err := h.listPaths(ctx, f)
	if err != nil {
		return nil, errors.Wrap(err, "listing manifests")
	}
	h.logger.Debug(ctx, "Found manifests", "manifests", manifestsPaths)

	manifests := make([]*remoteManifest, len(manifestsPaths))
	for i, mp := range manifestsPaths {
		manifests[i], err = h.readManifest(ctx, mp)
		if err != nil {
			return nil, errors.Wrapf(err, "reading manifest %s", mp)
		}
	}

	return manifests, nil
}

func (h *manifestV2Helper) DeleteManifest(ctx context.Context, m *remoteManifest) error {
	if !m.Temporary {
		h.logger.Info(ctx, "Delete manifest", "snapshot_tag", m.SnapshotTag)
	} else {
		h.logger.Info(ctx, "Delete orphaned temporary manifest", "snapshot_tag", m.SnapshotTag)
	}

	if err := h.deleteFile(ctx, m.RemoteSchemaFile()); err != nil {
		return errors.Wrap(err, "delete schema file")
	}

	if err := h.deleteFile(ctx, m.RemoteManifestFile()); err != nil {
		return errors.Wrap(err, "delete manifest file")
	}

	return nil
}

func (h *manifestV2Helper) deleteFile(ctx context.Context, path string) error {
	err := h.client.RcloneDeleteFile(ctx, h.host, h.location.RemotePath(path))
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		err = nil
	}
	return err
}

func (h *manifestV2Helper) readManifest(ctx context.Context, manifestPath string) (*remoteManifest, error) {
	m := &remoteManifest{}
	if err := m.ParsePartialPath(manifestPath); err != nil {
		return nil, err
	}

	// Load manifest
	b, err := h.client.RcloneCat(ctx, h.host, h.location.RemotePath(manifestPath))
	if err != nil {
		return nil, errors.Wrapf(err, "load manifest %s", manifestPath)
	}

	// Manifest is compressed
	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, errors.Wrapf(err, "uncompressing manifest")
	}

	if err := json.NewDecoder(gr).Decode(&m.Content); err != nil {
		return nil, errors.Wrapf(err, "parse manifest %s", manifestPath)
	}

	m.Location = h.location

	h.logger.Debug(ctx, "Loaded manifest",
		"host", h.host,
		"location", h.location,
		"path", manifestPath,
	)
	return m, nil
}

// listPaths return list of paths to manifests present under provided location.
func (h manifestV2Helper) listPaths(ctx context.Context, f ListFilter) ([]string, error) {
	// Filter out other clusters to speed up common case
	baseDir := path.Join("backup", string(metaDirKind))

	if f.ClusterID != uuid.Nil {
		if f.DC != "" {
			if f.NodeID != "" {
				baseDir = remoteManifestDir(f.ClusterID, f.DC, f.NodeID)
			} else {
				baseDir = path.Join(remoteMetaClusterDCDir(f.ClusterID), f.DC)
			}
		} else {
			baseDir = remoteMetaClusterDCDir(f.ClusterID)
		}
	}

	dirPrune := makeListFilterPruneDirFunc(f)
	w := walker{
		Host:     h.host,
		Location: h.location,
		Client:   h.client,
		PruneDir: dirPrune,
	}

	searchLevel := remoteManifestLevel(baseDir)
	h.logger.Debug(ctx, "Searching dirs", "base", baseDir, "level", searchLevel)
	dirs, err := w.DirsAtLevelN(ctx, baseDir, searchLevel)
	if err != nil {
		return nil, errors.Wrapf(err, "traversing dir %s on host %s", baseDir, h.host)
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
	parallelLimit := int(s/2 + 1)
	h.logger.Debug(ctx, "Parallel limit", "limit", parallelLimit)

	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		NoModTime: true,
		Recurse:   true,
	}

	err = parallel.Run(len(dirs), parallelLimit, func(i int) error {
		baseDir := dirs[i]

		files, err := h.client.RcloneListDir(ctx, h.host, h.location.RemotePath(baseDir), opts)
		if err != nil {
			return errors.Wrapf(err, "listing dir %s on host %s", baseDir, h.host)
		}

		for i := range files {
			// Filter out unwanted items
			p := path.Join(baseDir, files[i].Path)
			if dirPrune(p) {
				continue
			}
			m := &remoteManifest{}

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
			// Ignore temporary files
			if m.Temporary && !f.Temporary {
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
