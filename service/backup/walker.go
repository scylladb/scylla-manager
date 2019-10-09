// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"path"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/inexlist/ksfilter"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

var (
	walkerListFilesOpts = &scyllaclient.RcloneListDirOpts{
		NoModTime: true,
		FilesOnly: true,
	}
	walkerListDirsOpts = &scyllaclient.RcloneListDirOpts{
		DirsOnly:  true,
		NoModTime: true,
	}
)

type walker struct {
	Location Location
	Client   *scyllaclient.Client
	Prune    func(dir string) bool
}

func (w *walker) FilesAtLevel(ctx context.Context, host string, n int) ([]string, error) {
	return w.filesAtLevel(ctx, host, "", n)
}

func (w *walker) filesAtLevel(ctx context.Context, host string, dir string, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}

	if w.Prune != nil && w.Prune(dir) {
		return nil, nil
	}

	if n == 1 {
		files, err := w.Client.RcloneListDir(ctx, host, w.Location.RemotePath(dir), walkerListFilesOpts)
		if err != nil {
			return nil, err
		}
		return extractPaths(dir, files), nil
	}

	dirs, err := w.Client.RcloneListDir(ctx, host, w.Location.RemotePath(dir), walkerListDirsOpts)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, d := range dirs {
		p, err := w.filesAtLevel(ctx, host, path.Join(dir, d.Name), n-1)
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

func listManifests(ctx context.Context, location Location, client *scyllaclient.Client, host string, filter ListFilter, loadFiles bool) ([]remoteManifest, error) {
	f, err := makeListFilterPruneFunc(filter)
	if err != nil {
		return nil, err
	}

	w := walker{
		Location: location,
		Client:   client,
		Prune:    f,
	}

	files, err := w.FilesAtLevel(ctx, host, remoteManifestLevel())
	if err != nil {
		return nil, err
	}

	var (
		manifests []remoteManifest
		m         remoteManifest
	)
	for _, f := range files {
		// It's unlikely but the list may contain manifests and all it's sibling
		// files, we want to clear everything but the manifests.
		if err := m.ParsePartialPath(f); err != nil {
			continue
		}

		// Load the manifest files if needed
		if loadFiles {
			b, err := client.RcloneCat(ctx, host, location.RemotePath(m.remoteManifestFile()))
			if err != nil {
				return nil, errors.Wrap(err, "load manifest")
			}
			var v struct {
				Files []string `json:"files"`
			}
			if err := json.Unmarshal(b, &v); err != nil {
				return nil, errors.Wrap(err, "parse manifest")
			}
			m.Files = v.Files
		}

		manifests = append(manifests, m)
	}

	return manifests, nil
}

func makeListFilterPruneFunc(f ListFilter) (func(string) bool, error) {
	// Load filters
	ksf, err := ksfilter.NewFilter(f.Keyspace)
	if err != nil {
		return nil, err
	}

	var m remoteManifest
	return func(dir string) bool {
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
