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
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/atomic"
)

type purger struct {
	ClusterID      uuid.UUID
	TaskID         uuid.UUID
	Policy         int
	Client         *scyllaclient.Client
	HostInfo       hostInfo
	ManifestHelper manifestHelper
	Logger         log.Logger
}

func (p *purger) purge(ctx context.Context) error {
	p.Logger.Info(ctx, "Analysing",
		"host", p.HostInfo.IP,
		"location", p.HostInfo.Location,
	)

	// Load all manifests
	manifests, err := p.loadAllManifests(ctx)
	if err != nil {
		return errors.Wrap(err, "find and load remote manifests")
	}

	manifestTags := strset.New()
	for _, m := range manifests {
		manifestTags.Add(m.SnapshotTag)
	}
	tags := manifestTags.List()

	// Exit if there no tags to delete
	if len(tags) <= p.Policy {
		p.Logger.Debug(ctx, "Nothing to do")
		return nil
	}

	// Sort by date ascending
	sort.Strings(tags)

	// Select tags to delete
	staleTags := tags[:len(tags)-p.Policy]

	staleTagsSet := strset.New()
	for _, t := range staleTags {
		staleTagsSet.Add(t)
	}

	isStaleManifest := func(m *remoteManifest) bool {
		return m.TaskID == p.TaskID && staleTagsSet.Has(m.SnapshotTag)
	}

	// Select stale sst files in the form keyspace/<keyspace_name>/table/<table_name>/<version>
	staleFiles := strset.New()
	for i := range manifests {
		if isStaleManifest(manifests[i]) {
			for _, fi := range manifests[i].Content.Index {
				for _, f := range fi.Files {
					staleFiles.Add(ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f))
				}
			}
		}
	}
	// Remove alive files from staleFiles
	for i := range manifests {
		if !isStaleManifest(manifests[i]) {
			for _, fi := range manifests[i].Content.Index {
				for _, f := range fi.Files {
					staleFiles.Remove(ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f))
				}
			}
		}
	}

	// Skip if there are no orphan files
	if !staleFiles.IsEmpty() {
		// Delete sstables that are not alive
		p.Logger.Debug(ctx, "Stale files are", "files", staleFiles)
		staleFilesList := staleFiles.List()
		if err := p.deleteSSTables(ctx, staleFilesList); err != nil {
			return errors.Wrap(err, "delete stale data")
		}
	}

	// Delete stale tags
	for _, m := range manifests {
		if !staleTagsSet.Has(m.SnapshotTag) {
			continue
		}
		if err := p.ManifestHelper.DeleteManifest(ctx, m); err != nil {
			return errors.Wrap(err, "delete stale tag")
		}

		p.Logger.Info(ctx, "Deleted metadata according to retention policy",
			"host", p.HostInfo.IP,
			"location", p.HostInfo.Location,
			"tag", m.SnapshotTag,
			"policy", p.Policy,
			"size", m.Content.Size,
			"files", staleFiles.Size(),
		)
	}

	return nil
}

func (p *purger) loadAllManifests(ctx context.Context) ([]*remoteManifest, error) {
	filter := ListFilter{
		ClusterID: p.ClusterID,
		NodeID:    p.HostInfo.ID,
		DC:        p.HostInfo.DC,
	}
	return p.ManifestHelper.ListManifests(ctx, filter)
}

func (p *purger) deleteSSTables(ctx context.Context, files []string) error {
	baseDir := remoteSSTableBaseDir(p.ClusterID, p.HostInfo.DC, p.HostInfo.ID)

	deleted, err := p.deleteFiles(ctx, baseDir, files)
	if err != nil {
		p.Logger.Info(ctx, "Failed to delete orphaned data files",
			"host", p.HostInfo.IP,
			"location", p.HostInfo.Location,
			"files", deleted,
			"error", err,
		)
		return err
	}

	p.Logger.Info(ctx, "Deleted orphaned data files",
		"host", p.HostInfo.IP,
		"location", p.HostInfo.Location,
		"files", deleted,
	)

	return nil
}

func (p *purger) deleteFiles(ctx context.Context, baseDir string, files []string) (n int64, err error) {
	const limit = 5

	var deleted atomic.Int64

	err = parallel.Run(len(files), limit, func(i int) error {
		filePath := files[i]
		l := p.HostInfo.Location.RemotePath(path.Join(baseDir, filePath))
		if err := p.deleteFile(ctx, l); err != nil {
			return err
		}
		deleted.Inc()
		return nil
	})

	return deleted.Load(), err
}

func (p *purger) deleteFile(ctx context.Context, path string) error {
	p.Logger.Debug(ctx, "Deleting file", "host", p.HostInfo.IP, "path", path)
	err := p.Client.RcloneDeleteFile(ctx, p.HostInfo.IP, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
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
