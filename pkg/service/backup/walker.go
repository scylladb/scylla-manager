// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"

	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

var walkerListDirsOpts = &scyllaclient.RcloneListDirOpts{
	DirsOnly:  true,
	NoModTime: true,
}

// walker performs a recursive walk in location that is proxied over agent at
// host, it should be avoided as much possible.
type walker struct {
	Host     string
	Location backup.Location
	Client   *scyllaclient.Client
	PruneDir func(dir string) bool
}

func (w *walker) DirsAtLevelN(ctx context.Context, dir string, n int) ([]string, error) {
	if n <= 0 {
		return []string{dir}, nil
	}

	if w.PruneDir != nil && w.PruneDir(dir) {
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
