// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/internal/inexlist/ksfilter"
	"github.com/scylladb/mermaid/internal/parallel"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

var (
	walkerListDirsOpts = &scyllaclient.RcloneListDirOpts{
		DirsOnly:  true,
		NoModTime: true,
	}
)

// walker performs a recursive walk in location that is proxied over agent at
// host, it should be avoided as much possible.
type walker struct {
	Host     string
	Location Location
	Client   *scyllaclient.Client
	Prune    func(dir string) bool
}

func (w *walker) DirsAtLevelN(ctx context.Context, dir string, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}

	if w.Prune != nil && w.Prune(dir) {
		return nil, nil
	}

	if n == 1 {
		files, err := w.Client.RcloneListDir(ctx, w.Host, w.Location.RemotePath(dir), walkerListDirsOpts)
		if err != nil {
			return nil, err
		}
		return extractPaths(dir, files), nil
	}

	dirs, err := w.Client.RcloneListDir(ctx, w.Host, w.Location.RemotePath(dir), walkerListDirsOpts)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, d := range dirs {
		p, err := w.DirsAtLevelN(ctx, path.Join(dir, d.Name), n-1)
		if err != nil {
			return nil, err
		}
		paths = append(paths, p...)
	}

	return paths, nil
}

func extractPaths(baseDir string, items []*scyllaclient.RcloneListDirItem) (paths []string) {
	for _, i := range items {
		paths = append(paths, path.Join(baseDir, i.Path))
	}
	return
}

func listManifests(ctx context.Context, client *scyllaclient.Client, host string, l Location, filter ListFilter, load bool, logger log.Logger) ([]remoteManifest, error) {
	prune, err := makeListFilterPruneFunc(filter)
	if err != nil {
		return nil, errors.Wrap(err, "create filter")
	}

	w := walker{
		Host:     host,
		Location: l,
		Client:   client,
		Prune:    prune,
	}

	// Filter out other clusters to speed up common case
	baseDir := path.Join("backup", string(metaDirKind))
	if filter.ClusterID != uuid.Nil {
		baseDir = remoteMetaClusterDCDir(filter.ClusterID)
	}

	keyspaceDirs, err := w.DirsAtLevelN(ctx, baseDir, remoteMetaKeyspaceLevel(baseDir))
	if err != nil {
		return nil, errors.Wrapf(err, "%s", host)
	}
	logger.Debug(ctx, "Keyspace dirs", "size", len(keyspaceDirs), "dirs", keyspaceDirs)

	var (
		allManifests []remoteManifest
		mu           sync.Mutex
	)

	// Deduce parallelism level from nr. of shards
	s, err := client.ShardCount(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "get shard count")
	}
	parallelLimit := int(s/2 + 1)
	logger.Debug(ctx, "Parallel limit", "limit", parallelLimit)

	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		NoModTime: true,
		Recurse:   true,
	}

	err = parallel.Run(len(keyspaceDirs), parallelLimit, func(i int) error {
		baseDir := keyspaceDirs[i]

		files, err := client.RcloneListDir(ctx, host, l.RemotePath(baseDir), opts)
		if err != nil {
			return errors.Wrapf(err, "%s", host)
		}

		// Read manifests
		var manifests []remoteManifest
		for _, f := range files {
			// Filter out unwanted items
			p := path.Join(baseDir, f.Path)
			if prune(p) {
				continue
			}

			var m remoteManifest
			// It's unlikely but the list may contain manifests and all its
			// sibling files, we want to clear everything but the manifests.
			if err := m.ParsePartialPath(p); err != nil {
				logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
					"host", host,
					"location", l,
					"path", p,
				)
				continue
			}

			if load {
				// Set location
				m.Location = l

				// Load manifest
				b, err := client.RcloneCat(ctx, host, l.RemotePath(m.RemoteManifestFile()))
				if err != nil {
					return errors.Wrapf(err, "load manifest %s", m.RemoteManifestFile())
				}
				var v struct {
					Files []string `json:"files"`
				}
				if err := json.Unmarshal(b, &v); err != nil {
					return errors.Wrapf(err, "parse manifest %s", m.RemoteManifestFile())
				}
				m.Files = v.Files
				logger.Debug(ctx, "Loaded manifest",
					"host", host,
					"location", l,
					"path", m.RemoteManifestFile(),
					"files", m.Files,
				)

				// Filter files based on manifest
				files, err := client.RcloneListDir(ctx, host, l.RemotePath(m.RemoteSSTableVersionDir()), nil)
				if err != nil {
					return errors.Wrap(err, "list sstables")
				}
				s := strset.New(extractGroupingKeys(m)...)
				for _, f := range files {
					k, err := groupingKey(path.Join(m.Version, f.Path))
					if err != nil {
						logger.Debug(ctx, "GroupingKey error", "error", err)
					}
					if s.Has(k) {
						m.FilesExpanded = append(m.FilesExpanded, f.Name)
					}
				}
			}

			manifests = append(manifests, m)
		}
		logger.Debug(ctx, "Manifests", "dir", baseDir, "manifests", len(manifests))

		// Update all manifests
		mu.Lock()
		allManifests = append(allManifests, manifests...)
		mu.Unlock()

		return nil
	})
	if err != nil {
		return nil, err
	}
	logger.Debug(ctx, "All manifests", "manifests", len(allManifests))

	return allManifests, nil
}

func makeListFilterPruneFunc(f ListFilter) (func(string) bool, error) {
	// Load filters
	ksf, err := ksfilter.NewFilter(f.Keyspace)
	if err != nil {
		return nil, err
	}

	return func(dir string) bool {
		var m remoteManifest

		// Discard invalid paths
		if err := m.ParsePartialPath(dir); err != nil {
			return true
		}
		// Filter cluster
		if m.ClusterID != uuid.Nil && f.ClusterID != uuid.Nil {
			if m.ClusterID != f.ClusterID {
				return true
			}
		}
		// Filter keyspace and table
		if m.Keyspace != "" && m.Table != "" && len(f.Keyspace) > 0 {
			if !ksf.Check(m.Keyspace, m.Table) {
				return true
			}
		}
		// Filter snapshot tags
		if m.SnapshotTag != "" {
			if f.SnapshotTag != "" {
				return m.SnapshotTag != f.SnapshotTag
			}
			if !f.MinDate.IsZero() && m.SnapshotTag < snapshotTagAt(f.MinDate) {
				return true
			}
			if !f.MaxDate.IsZero() && m.SnapshotTag > snapshotTagAt(f.MaxDate) {
				return true
			}
		}

		return false
	}, nil
}
