// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/atomic"
)

type purger struct {
	Client         *scyllaclient.Client
	Filter         ListFilter
	Host           string
	Location       backup.Location
	ManifestHelper manifestHelper
	Logger         log.Logger
}

// PurgeSnapshot allows to delete data and metadata associated
// with provided snapshotTag.
func (p *purger) PurgeSnapshot(ctx context.Context, snapshotTag string) error {
	p.Logger.Info(ctx, "Purging snapshot",
		"location", p.Location,
		"snapshot_tag", snapshotTag,
	)

	// Load manifests from all tasks
	manifests, err := p.loadAllManifests(ctx, false)
	if err != nil {
		return errors.Wrap(err, "find and load remote manifests")
	}

	found := false
	for _, m := range manifests {
		if m.SnapshotTag == snapshotTag {
			found = true
			break
		}
	}
	if !found {
		return service.ErrNotFound
	}

	isStaleManifest := func(m *backup.RemoteManifest) bool {
		return m.SnapshotTag == snapshotTag
	}

	return p.purge(ctx, manifests, isStaleManifest)
}

// PurgeTask analyzes existing manifests and keeps only `policy` latest task snapshots.
// Expired snapshots are removed from storage.
func (p *purger) PurgeTask(ctx context.Context, taskID uuid.UUID, policy int) error {
	p.Logger.Info(ctx, "Analysing", "location", p.Location)

	// Load manifests from all tasks
	manifests, err := p.loadAllManifests(ctx, true)
	if err != nil {
		return errors.Wrap(err, "find and load remote manifests")
	}

	manifestTaskTags := strset.New()
	for _, m := range manifests {
		if !m.Temporary && m.TaskID == taskID {
			manifestTaskTags.Add(m.SnapshotTag)
		}
	}
	taskTags := manifestTaskTags.List()

	// Exit if there are no tags to purge
	if len(taskTags) <= policy {
		p.Logger.Info(ctx, "No snapshots to purge", "snapshot_count", len(taskTags), "policy", policy)
		return nil
	}

	// Sort by date ascending
	sort.Strings(taskTags)
	// Select tags to delete
	staleTags := taskTags[:len(taskTags)-policy]

	p.Logger.Info(ctx, "Found snapshots to delete",
		"snapshot_count", len(taskTags),
		"policy", policy,
		"delete", staleTags,
	)

	staleTagsSet := strset.New(staleTags...)
	isStaleManifest := func(m *backup.RemoteManifest) bool {
		return m.Temporary || staleTagsSet.Has(m.SnapshotTag)
	}

	return p.purge(ctx, manifests, isStaleManifest)
}

// loadAllManifests loads manifests belonging to all tasks.
func (p *purger) loadAllManifests(ctx context.Context, temporary bool) ([]*backup.RemoteManifest, error) {
	f := p.Filter
	f.TaskID = uuid.Nil
	f.Temporary = temporary
	return p.ManifestHelper.ListManifests(ctx, f)
}

func (p *purger) purge(ctx context.Context, manifests []*backup.RemoteManifest, isStaleManifest func(*backup.RemoteManifest) bool) error {
	// Select stale sst files in the form of full path to file.
	staleFiles := strset.New()

	// Add files from manifests to be deleted.
	p.forEachFile(p.filterManifests(manifests, isStaleManifest), func(path string) {
		staleFiles.Add(path)
	})
	// From that remove files from active manifests.
	isActiveManifest := func(m *backup.RemoteManifest) bool {
		return !isStaleManifest(m)
	}
	p.forEachFile(p.filterManifests(manifests, isActiveManifest), func(path string) {
		staleFiles.Remove(path)
	})

	// Delete SSTables
	deletedFiles, err := p.deleteFiles(ctx, staleFiles)
	if err != nil {
		return errors.Wrap(err, "delete files")
	}

	// Delete manifests
	var deletedManifests int
	for _, m := range p.filterManifests(manifests, isStaleManifest) {
		if err := p.ManifestHelper.DeleteManifest(ctx, m); err != nil {
			return errors.Wrap(err, "delete manifest")
		}
		deletedManifests++
	}

	p.Logger.Info(ctx, "Deleted", "files", deletedFiles, "manifests", deletedManifests)

	return nil
}

func (p *purger) filterManifests(manifests []*backup.RemoteManifest, filter func(*backup.RemoteManifest) bool) (out []*backup.RemoteManifest) {
	for _, m := range manifests {
		if filter(m) {
			out = append(out, m)
		}
	}
	return
}

func (p *purger) forEachFile(manifests []*backup.RemoteManifest, callback func(path string)) {
	for _, m := range manifests {
		baseDir := backup.RemoteSSTableBaseDir(m.ClusterID, m.DC, m.NodeID)
		for _, fi := range m.Content.Index {
			for _, f := range fi.Files {
				callback(path.Join(baseDir, ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f)))
			}
		}
	}
}

func (p *purger) deleteFiles(ctx context.Context, staleFiles *strset.Set) (int64, error) {
	if staleFiles.Size() == 0 {
		return 0, nil
	}

	const (
		limit   = 5
		logEach = 100
	)

	var (
		// mu protects staleFiles concurrent access
		mu sync.Mutex

		total   = staleFiles.Size()
		success atomic.Int64
	)
	err := parallel.Run(total, limit, func(i int) error {
		mu.Lock()
		f := staleFiles.Pop()
		mu.Unlock()

		if err := p.deleteFile(ctx, p.Location.RemotePath(f)); err != nil {
			return err
		}

		cur := success.Inc()
		if cur >= logEach && cur%logEach == 0 {
			p.Logger.Info(ctx, "Deleted files", "count", cur, "total", total)
		}

		return nil
	})

	return success.Load(), err
}

func (p *purger) deleteFile(ctx context.Context, path string) error {
	p.Logger.Debug(ctx, "Deleting file", "path", path)
	err := p.Client.RcloneDeleteFile(ctx, p.Host, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		p.Logger.Info(ctx, "File missing on delete", "path", path)
		err = nil
	}
	return err
}
