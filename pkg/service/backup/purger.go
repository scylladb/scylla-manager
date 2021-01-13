// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
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
	Location       Location
	ManifestHelper manifestHelper
	Logger         log.Logger
}

// PurgeSnapshot allows to delete data and metadata associated
// with provided snapshotTag.
func (p *purger) PurgeSnapshot(ctx context.Context, snapshotTag string) error {
	p.Logger.Info(ctx, "Purging snapshot on host",
		"host", p.Host,
		"location", p.Location,
		"snapshot_tag", snapshotTag,
	)

	// Load manifests from all tasks
	manifests, err := p.loadAllManifests(ctx)
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

	isStaleManifest := func(m *remoteManifest) bool {
		return m.SnapshotTag == snapshotTag
	}

	return p.purge(ctx, manifests, isStaleManifest)
}

// PurgeTask analyzes existing manifests and keeps only `policy` latest task snapshots.
// Expired snapshots are removed from storage.
func (p *purger) PurgeTask(ctx context.Context, taskID uuid.UUID, policy int) error {
	p.Logger.Info(ctx, "Analysing",
		"host", p.Host,
		"location", p.Location,
	)

	// Load manifests from all tasks
	manifests, err := p.loadAllManifests(ctx)
	if err != nil {
		return errors.Wrap(err, "find and load remote manifests")
	}

	manifestTaskTags := strset.New()
	for _, m := range manifests {
		if m.TaskID == taskID {
			manifestTaskTags.Add(m.SnapshotTag)
		}
	}
	taskTags := manifestTaskTags.List()

	// Exit if there are no tags to purge
	if len(taskTags) <= policy {
		p.Logger.Debug(ctx, "Nothing to do")
		return nil
	}

	// Sort by date ascending
	sort.Strings(taskTags)

	// Select tags to delete
	staleTags := taskTags[:len(taskTags)-policy]

	staleTagsSet := strset.New(staleTags...)
	isStaleManifest := func(m *remoteManifest) bool {
		return staleTagsSet.Has(m.SnapshotTag)
	}

	return p.purge(ctx, manifests, isStaleManifest)
}

func (p *purger) purge(ctx context.Context, manifests []*remoteManifest, isStaleManifest func(*remoteManifest) bool) error {
	// Select stale sst files in the form of full path to file.
	staleFiles := strset.New()
	for _, m := range manifests {
		if isStaleManifest(m) {
			baseDir := remoteSSTableBaseDir(m.ClusterID, m.DC, m.NodeID)
			for _, fi := range m.Content.Index {
				for _, f := range fi.Files {
					staleFiles.Add(path.Join(baseDir, ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f)))
				}
			}
		}
	}
	// Remove alive files from staleFiles
	for _, m := range manifests {
		if !isStaleManifest(m) {
			baseDir := remoteSSTableBaseDir(m.ClusterID, m.DC, m.NodeID)
			for _, fi := range m.Content.Index {
				for _, f := range fi.Files {
					staleFiles.Remove(path.Join(baseDir, ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f)))
				}
			}
		}
	}

	// Skip if there are no orphan files
	if !staleFiles.IsEmpty() {
		// Delete sstables that are not alive
		p.Logger.Debug(ctx, "Stale files are", "files", staleFiles)

		if err := p.deleteSSTables(ctx, staleFiles); err != nil {
			return errors.Wrap(err, "delete stale data")
		}
	}

	// Delete stale tags
	for _, m := range manifests {
		if isStaleManifest(m) {
			if err := p.ManifestHelper.DeleteManifest(ctx, m); err != nil {
				return errors.Wrap(err, "delete stale tag")
			}

			p.Logger.Info(ctx, "Deleted metadata according to retention policy",
				"host", p.Host,
				"location", p.Location,
				"tag", m.SnapshotTag,
				"size", m.Content.Size,
				"files", staleFiles.Size(),
			)
		}
	}

	return nil
}

// loadAllManifests loads manifests belonging to all tasks.
func (p *purger) loadAllManifests(ctx context.Context) ([]*remoteManifest, error) {
	filter := p.Filter
	filter.TaskID = uuid.Nil
	return p.ManifestHelper.ListManifests(ctx, filter)
}

func (p *purger) deleteSSTables(ctx context.Context, staleFiles *strset.Set) error {
	deleted, err := p.deleteFiles(ctx, staleFiles.List())
	if err != nil {
		p.Logger.Error(ctx, "Failed to delete orphaned data files",
			"host", p.Host,
			"location", p.Location,
			"files", deleted,
			"error", err,
		)
		return err
	}

	p.Logger.Info(ctx, "Deleted orphaned data files",
		"host", p.Host,
		"location", p.Location,
		"files", deleted,
	)

	return nil
}

func (p *purger) deleteFiles(ctx context.Context, files []string) (n int64, err error) {
	const limit = 5

	var deleted atomic.Int64

	err = parallel.Run(len(files), limit, func(i int) error {
		l := p.Location.RemotePath(files[i])
		if err := p.deleteFile(ctx, l); err != nil {
			return err
		}
		deleted.Inc()
		return nil
	})

	return deleted.Load(), err
}

func (p *purger) deleteFile(ctx context.Context, path string) error {
	p.Logger.Debug(ctx, "Deleting file", "host", p.Host, "path", path)
	err := p.Client.RcloneDeleteFile(ctx, p.Host, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		p.Logger.Info(ctx, "File missing on delete", "host", p.Host, "path", path)
		err = nil
	}
	return err
}

// newPurgerManifestHelper returns manifestHelper for purging purposes. Purging
// should support V2 manifest listing, and V1 + V2 purging. We list only V2
// manifests, because V1 manifests are migrated to V2 during backup procedure,
// so there is not point of listing them. Although once backup snapshot expires
// we would like to delete both of the versions.
func newPurgerManifestHelper(host string, location Location, client *scyllaclient.Client,
	logger log.Logger) manifestHelper {
	return &struct {
		manifestLister
		manifestDeleter
	}{
		manifestLister:  newManifestV2Helper(host, location, client, logger),
		manifestDeleter: newMultiVersionManifestDeleter(host, location, client, logger),
	}
}
