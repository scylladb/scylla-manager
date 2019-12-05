// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"regexp"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/multierr"
)

type purger struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	Keyspace  string
	Table     string
	Policy    int
	Client    *scyllaclient.Client
	Logger    log.Logger
}

func (p *purger) purge(ctx context.Context, h hostInfo) error {
	p.Logger.Info(ctx, "Analysing",
		"host", h.IP,
		"keyspace", p.Keyspace,
		"table", p.Table,
		"location", h.Location,
	)

	// Get list of stale tags that need to be deleted
	tags, err := p.listTaskTags(ctx, h)
	if err != nil {
		return errors.Wrap(err, "list remote task tags")
	}

	// Exit if there no tags to delete
	if len(tags) <= p.Policy {
		p.Logger.Debug(ctx, "Nothing to do")
		return nil
	}

	// Select tags to delete
	staleTags := tags[:len(tags)-p.Policy]

	// Load all manifests for the table
	manifests, err := p.loadAllManifests(ctx, h)
	if err != nil {
		return errors.Wrap(err, "find and load remote manifests")
	}

	// Select live sst files in the form version/la-xx-big
	idx := strset.New()
	for _, t := range staleTags {
		idx.Add(t)
	}
	aliveFiles := strset.New()
	staleFiles := strset.New()
	for _, m := range manifests {
		var s *strset.Set
		if m.TaskID == p.TaskID && idx.Has(m.SnapshotTag) {
			s = staleFiles
		} else {
			s = aliveFiles
		}
		s.Add(extractGroupingKeys(m)...)
	}

	// Remove alive files from stale files laving only the orphans
	staleFiles.Separate(aliveFiles)

	// Exit if there are no orphan files
	if staleFiles.IsEmpty() {
		p.Logger.Debug(ctx, "Nothing to do, no stale files")
		return nil
	}

	// Delete stale tags
	if err := p.deleteTags(ctx, h, staleTags); err != nil {
		return errors.Wrap(err, "delete stale tags")
	}

	// Delete sstables that are not alive (by grouping key)
	p.Logger.Debug(ctx, "Alive files are", "files", aliveFiles)

	isNotAlive := func(key string) bool {
		return !aliveFiles.Has(key)
	}
	if err := p.deleteSSTables(ctx, h, isNotAlive); err != nil {
		return errors.Wrap(err, "delete stale data")
	}

	return nil
}

// listTaskTags returns a sorted list of tags for the task being purged.
// The old tags are at the beginning of the returned slice.
func (p *purger) listTaskTags(ctx context.Context, h hostInfo) ([]string, error) {
	baseDir := remoteTagsDir(p.ClusterID, p.TaskID, h.DC, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Listing tags",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), nil)
	if err != nil {
		return nil, err
	}

	var tags []string
	for _, f := range files {
		if !f.IsDir {
			p.Logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"size", f.Size,
			)
			continue
		}

		tag := f.Name
		if !isSnapshotTag(tag) {
			p.Logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"size", f.Size,
			)
			continue
		}
		tags = append(tags, tag)
	}

	// Sort tags by date ascending
	sort.Strings(tags)

	return tags, nil
}

var taskTagVersionManifestRegexp = regexp.MustCompile("/([a-f0-9\\-]{36})/tag/(sm_[0-9]{14}UTC)/([a-f0-9]{32})/" + manifest + "$")

// loadAllManifests returns manifests for all the tasks and tags for the given
// kayspace and table.
func (p *purger) loadAllManifests(ctx context.Context, h hostInfo) ([]remoteManifest, error) {
	baseDir := remoteTasksDir(p.ClusterID, h.DC, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Loading all manifests",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	opts := &scyllaclient.RcloneListDirOpts{
		Recurse: true,
	}
	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), opts)
	if err != nil {
		return nil, err
	}

	var manifests []remoteManifest
	for _, f := range files {
		m := taskTagVersionManifestRegexp.FindStringSubmatch("/" + f.Path)
		if m == nil {
			// Report any unexpected files
			if !f.IsDir {
				p.Logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
					"host", h.IP,
					"location", h.Location,
					"path", path.Join(baseDir, f.Path),
					"size", f.Size,
				)
			}
			continue
		}

		var taskID uuid.UUID
		if err := taskID.UnmarshalText([]byte(m[1])); err != nil {
			p.Logger.Error(ctx, "Failed to parse task ID, ignoring file",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"error", err,
			)
			continue
		}

		v := remoteManifest{
			TaskID:      taskID,
			SnapshotTag: m[2],
			Version:     m[3],
		}

		p.Logger.Debug(ctx, "Found manifest",
			"host", h.IP,
			"location", h.Location,
			"path", path.Join(baseDir, f.Path),
		)

		v.Files, err = p.loadManifest(ctx, h, path.Join(baseDir, f.Path))
		if err != nil {
			return nil, errors.Wrap(err, "load manifest")
		}

		manifests = append(manifests, v)
	}

	return manifests, nil
}

func (p *purger) loadManifest(ctx context.Context, h hostInfo, path string) ([]string, error) {
	p.Logger.Debug(ctx, "Loading manifest",
		"host", h.IP,
		"location", h.Location,
		"path", path,
	)

	b, err := p.Client.RcloneCat(ctx, h.IP, h.Location.RemotePath(path))
	if err != nil {
		return nil, err
	}

	var v struct {
		Files []string `json:"files"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, errors.Wrap(err, "parse manifest")
	}

	p.Logger.Debug(ctx, "Loaded manifest",
		"host", h.IP,
		"location", h.Location,
		"path", path,
		"files", v.Files,
	)

	return v.Files, nil
}

func (p *purger) deleteSSTables(ctx context.Context, h hostInfo, filter func(key string) bool) error {
	baseDir := remoteSSTableDir(p.ClusterID, h.DC, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Listing sstables",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	opts := &scyllaclient.RcloneListDirOpts{
		Recurse:   true,
		FilesOnly: true,
	}
	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), opts)
	if err != nil {
		return err
	}

	var (
		errs        error
		deleted     int
		deletedSize int64
	)
	for _, f := range files {
		if f.IsDir {
			continue
		}
		key, err := groupingKey(f.Path)
		if err != nil {
			p.Logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"size", f.Size,
			)
			continue
		}
		if !filter(key) {
			continue
		}

		l := h.Location.RemotePath(path.Join(baseDir, f.Path))
		if err := p.deleteFile(ctx, h.IP, l); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "delete file %s", l))
		} else {
			deleted++
			deletedSize += f.Size
		}
	}

	p.Logger.Info(ctx, "Deleted orphaned data files",
		"host", h.IP,
		"keyspace", p.Keyspace,
		"table", p.Table,
		"location", h.Location,
		"files", deleted,
		"size", deletedSize,
	)

	return errs
}

func (p *purger) deleteTags(ctx context.Context, h hostInfo, tags []string) error {
	var (
		errs    error
		deleted int
	)
	for _, t := range tags {
		dir := remoteTagDir(p.ClusterID, p.TaskID, t, h.DC, h.ID, p.Keyspace, p.Table)
		l := h.Location.RemotePath(dir)
		if err := p.deleteDir(ctx, h.IP, l); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "delete directory %s", l))
		} else {
			deleted++
		}
	}

	p.Logger.Info(ctx, "Deleted metadata according to retention policy",
		"host", h.IP,
		"keyspace", p.Keyspace,
		"table", p.Table,
		"location", h.Location,
		"tags", deleted,
		"policy", p.Policy,
	)

	return errs
}

func (p *purger) deleteFile(ctx context.Context, ip, path string) error {
	p.Logger.Debug(ctx, "Deleting file", "host", ip, "path", path)
	err := p.Client.RcloneDeleteFile(ctx, ip, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		err = nil
	}
	return err
}

func (p *purger) deleteDir(ctx context.Context, ip, path string) error {
	p.Logger.Debug(ctx, "Deleting directory", "host", ip, "path", path)
	err := p.Client.RcloneDeleteDir(ctx, ip, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		err = nil
	}
	return err
}
