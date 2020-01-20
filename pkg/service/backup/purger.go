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
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/multierr"
)

type purger struct {
	ClusterID      uuid.UUID
	TaskID         uuid.UUID
	Policy         int
	Client         *scyllaclient.Client
	SnapshotDirs   []snapshotDir
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

	// Exit if there are no orphan files
	if staleFiles.IsEmpty() {
		p.Logger.Debug(ctx, "Nothing to do, no stale files")
		return nil
	}

	// Delete stale tags
	for _, m := range manifests {
		if staleTagsSet.Has(m.SnapshotTag) {
			if err := p.ManifestHelper.DeleteManifest(ctx, m); err != nil {
				return errors.Wrap(err, "delete stale tag")
			}

			p.Logger.Info(ctx, "Deleted metadata according to retention policy",
				"host", p.HostInfo.IP,
				"location", p.HostInfo.Location,
				"tags", m.SnapshotTag,
				"policy", p.Policy,
			)
		}
	}

	// Delete sstables that are not alive
	p.Logger.Debug(ctx, "Stale files are", "files", staleFiles)
	if err := p.deleteSSTables(ctx, staleFiles); err != nil {
		return errors.Wrap(err, "delete stale data")
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

func (p *purger) deleteSSTables(ctx context.Context, filePaths *strset.Set) error {
	baseDir := remoteSSTableBaseDir(p.ClusterID, p.HostInfo.DC, p.HostInfo.ID)

	p.Logger.Debug(ctx, "Listing sstables",
		"host", p.HostInfo.IP,
		"location", p.HostInfo.Location,
		"path", baseDir,
	)

	var (
		errs    error
		deleted int
	)

	filePaths.Each(func(filePath string) bool {
		l := p.HostInfo.Location.RemotePath(path.Join(baseDir, filePath))
		if err := p.deleteFile(ctx, l); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "delete file %s", l))
		} else {
			deleted++
		}
		return true
	})

	p.Logger.Info(ctx, "Deleted orphaned data files",
		"host", p.HostInfo.IP,
		"location", p.HostInfo.Location,
		"files", deleted,
	)

	return errs
}

func (p *purger) deleteFile(ctx context.Context, path string) error {
	p.Logger.Debug(ctx, "Deleting file", "host", p.HostInfo.IP, "path", path)
	err := p.Client.RcloneDeleteFile(ctx, p.HostInfo.IP, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		err = nil
	}
	return err
}
