// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/b16set"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

type remoteManifest struct {
	TaskID  uuid.UUID
	RunID   uuid.UUID
	Version string
	Files   []string
}

const manifestFileSuffix = "-Data.db"

func (m remoteManifest) extractFilesGroupingKeys() []string {
	var s []string
	for _, f := range m.Files {
		v := path.Join(m.Version, strings.TrimSuffix(f, manifestFileSuffix))
		s = append(s, v)
	}
	return s
}

// used to get groupingKey
var tableVersionFileNamePattern = regexp.MustCompile("^[a-f0-9]{32}/[a-z]{2}-[0-9]+-big")

func groupingKey(file string) (string, error) {
	m := tableVersionFileNamePattern.FindStringSubmatch(file)
	if m == nil {
		return "", errors.New("file path does not match a pattern")
	}
	return m[0], nil
}

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
	// Get list of stale runs that need to be deleted
	runs, err := p.listTaskRuns(ctx, h)
	if err != nil {
		return errors.Wrap(err, "failed to list remote task runs")
	}
	// Exit if there no runs to delete
	if len(runs) <= p.Policy {
		p.Logger.Debug(ctx, "Nothing to do")
		return nil
	}
	// Select runs to delete
	staleRuns := runs[:len(runs)-p.Policy]

	// Load all manifests for the table
	manifests, err := p.loadAllManifests(ctx, h)
	if err != nil {
		return errors.Wrap(err, "failed to find and load remote manifests")
	}

	// Select live sst files in the form version/la-xx-big
	idx := b16set.New()
	for _, r := range staleRuns {
		idx.Add(r.Bytes16())
	}
	aliveFiles := strset.New()
	staleFiles := strset.New()
	for _, m := range manifests {
		var s *strset.Set
		if m.TaskID == p.TaskID && idx.Has(m.RunID.Bytes16()) {
			s = staleFiles
		} else {
			s = aliveFiles
		}
		s.Add(m.extractFilesGroupingKeys()...)
	}
	// Remove alive files from stale files laving only the orphans
	staleFiles.Separate(aliveFiles)
	// Exit if there are no orphan files
	if staleFiles.IsEmpty() {
		p.Logger.Debug(ctx, "Nothing to do, no stale files")
		return nil
	}

	// Delete sstables that are not alive (by grouping key)
	p.Logger.Debug(ctx, "Alive files are", "files", aliveFiles)

	isNotAlive := func(key string) bool {
		return !aliveFiles.Has(key)
	}
	if err := p.deleteSSTables(ctx, h, isNotAlive); err != nil {
		return errors.Wrap(err, "failed to delete stale sstables")
	}

	// Delete stale runs
	if err := p.deleteRuns(ctx, h, staleRuns); err != nil {
		return errors.Wrap(err, "failed to delete stale runs")
	}

	return nil
}

// listTaskRuns returns a sorted list of run IDs for the task being purged.
// The old runs are at the beginning of the returned slice.
func (p *purger) listTaskRuns(ctx context.Context, h hostInfo) ([]uuid.UUID, error) {
	baseDir := remoteRunsDir(p.ClusterID, p.TaskID, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Listing runs",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), false)
	if err != nil {
		return nil, err
	}

	var ids []uuid.UUID
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
		var runID uuid.UUID
		if err := runID.UnmarshalText([]byte(f.Name)); err != nil {
			p.Logger.Error(ctx, "Detected unexpected file, it does not belong to Scylla",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"size", f.Size,
			)
			continue
		}
		ids = append(ids, runID)
	}

	sort.Slice(ids, func(i, j int) bool {
		return uuid.Compare(ids[i], ids[j]) < 0
	})

	return ids, nil
}

// loadAllManifests returns manifests for all the tasks and runs for the given
// kayspace and table.
func (p *purger) loadAllManifests(ctx context.Context, h hostInfo) ([]remoteManifest, error) {
	baseDir := remoteTasksDir(p.ClusterID, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Loading all manifests",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), true)
	if err != nil {
		return nil, err
	}

	r := regexp.MustCompile("/([a-f0-9\\-]{36})/run/([a-f0-9\\-]{36})/([a-f0-9]{32})/" + manifest + "$")

	var manifests []remoteManifest
	for _, f := range files {
		m := r.FindStringSubmatch("/" + f.Path)
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
		var runID uuid.UUID
		if err := runID.UnmarshalText([]byte(m[2])); err != nil {
			p.Logger.Error(ctx, "Failed to parse run ID, ignoring file",
				"host", h.IP,
				"location", h.Location,
				"path", path.Join(baseDir, f.Path),
				"error", err,
			)
			continue
		}
		version := m[3]

		v := remoteManifest{
			TaskID:  taskID,
			RunID:   runID,
			Version: version,
		}

		p.Logger.Debug(ctx, "Found manifest",
			"host", h.IP,
			"location", h.Location,
			"path", path.Join(baseDir, f.Path),
		)

		v.Files, err = p.loadManifest(ctx, h, path.Join(baseDir, f.Path))
		if err != nil {
			return nil, errors.Wrap(err, "failed to load manifest")
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
		return nil, errors.Wrap(err, "failed to parse manifest")
	}

	return v.Files, nil
}

func (p *purger) deleteSSTables(ctx context.Context, h hostInfo, filter func(key string) bool) error {
	baseDir := remoteSSTableDir(p.ClusterID, h.ID, p.Keyspace, p.Table)

	p.Logger.Debug(ctx, "Listing sstables",
		"host", h.IP,
		"location", h.Location,
		"path", baseDir,
	)

	files, err := p.Client.RcloneListDir(ctx, h.IP, h.Location.RemotePath(baseDir), true)
	if err != nil {
		return err
	}

	var errs error
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

		p.Logger.Info(ctx, "Deleting sstable file",
			"host", h.IP,
			"location", h.Location,
			"path", path.Join(baseDir, f.Path),
			"size", f.Size,
		)

		l := h.Location.RemotePath(path.Join(baseDir, f.Path))
		errs = multierr.Append(
			errs,
			errors.Wrapf(p.deleteFile(ctx, h.IP, l), "failed to delete file %s", l),
		)
	}

	return errs
}

func (p *purger) deleteRuns(ctx context.Context, h hostInfo, runs []uuid.UUID) error {
	var errs error
	for _, run := range runs {
		dir := remoteRunDir(p.ClusterID, p.TaskID, run, h.ID, p.Keyspace, p.Table)
		p.Logger.Info(ctx, "Deleting run directory",
			"host", h.IP,
			"location", h.Location,
			"path", dir,
		)
		l := h.Location.RemotePath(dir)
		errs = multierr.Append(
			errs,
			errors.Wrapf(p.deleteDir(ctx, h.IP, l), "failed to delete directory %s", l),
		)
	}

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
