// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/atomic"
)

// staleTags returns collection of snapshot tags for manifests that are "stale".
// That is:
// - temporary manifests,
// - manifests over task days retention days policy,
// - manifests over task retention policy,
// - manifests older than threshold if retention policy is unknown.
// Moreover, it returns the oldest snapshot tag time that remains undeleted by retention policy.
func staleTags(manifests []*ManifestInfo, retentionMap RetentionMap) (*strset.Set, time.Time, error) {
	tags := strset.New()
	var oldest time.Time

	for taskID, taskManifests := range groupManifestsByTask(manifests) {
		taskPolicy := GetRetention(taskID, retentionMap)
		taskTags := strset.New()
		for _, m := range taskManifests {
			t, err := SnapshotTagTime(m.SnapshotTag)
			if err != nil {
				return nil, time.Time{}, errors.Wrapf(err, "parse manifest snapshot tag time")
			}

			switch {
			case m.Temporary:
				tags.Add(m.SnapshotTag)
			// Tasks can have a Retention policy and a RetentionDays policy so fall through if tag is not too old
			case taskPolicy.RetentionDays > 0 && t.Before(timeutc.Now().AddDate(0, 0, -taskPolicy.RetentionDays)):
				tags.Add(m.SnapshotTag)
			case taskPolicy.Retention > 0:
				taskTags.Add(m.SnapshotTag)
			case t.Before(oldest) || oldest.IsZero():
				oldest = t
			}
		}

		if taskPolicy.Retention > 0 && taskTags.Size()-taskPolicy.Retention > 0 {
			l := taskTags.List()
			sort.Strings(l)
			cut := len(l) - taskPolicy.Retention
			tags.Add(l[:cut]...)

			t, err := SnapshotTagTime(l[cut])
			if err != nil {
				return nil, time.Time{}, err
			}
			if t.Before(oldest) || oldest.IsZero() {
				oldest = t
			}
		}
	}

	return tags, oldest, nil
}

type purger struct {
	client *scyllaclient.Client
	host   string
	logger log.Logger
	// nodeIP maps node ID to IP based on information from read manifests.
	nodeIP map[string]string

	notifyEach int
	OnScan     func(scanned, orphaned int, orphanedBytes int64)
	OnDelete   func(total, success int)
}

func newPurger(client *scyllaclient.Client, host string, logger log.Logger) purger {
	return purger{
		client: client,
		host:   host,
		logger: logger,
		nodeIP: make(map[string]string),

		notifyEach: 1000,
	}
}

// PurgeSnapshotTags removes files that are no longer needed as given snapshot tags are purged.
// Oldest represents the time of the oldest backup that we intend to keep - it is used to purge versioned files.
func (p purger) PurgeSnapshotTags(ctx context.Context, manifests []*ManifestInfo, tags *strset.Set, oldest time.Time) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}

	var (
		// Used to obtain values which are common for all manifests in this function call
		// (e.g. location, clusterID, nodeID, ...)
		anyM  = manifests[0]
		files = make(fileSet)
		stale = 0
	)

	for _, m := range manifests {
		if tags.Has(m.SnapshotTag) {
			stale++
			p.logger.Info(ctx, "Found manifest to remove",
				"task", m.TaskID,
				"snapshot_tag", m.SnapshotTag,
				"temporary", m.Temporary,
			)
			if err := p.forEachDirInManifest(ctx, m, files.AddFiles); err != nil {
				return 0, errors.Wrapf(err, "load manifest (snapshot) %s", m.Path())
			}
		}
	}
	if stale == 0 {
		return 0, nil
	}
	for _, m := range manifests {
		if !tags.Has(m.SnapshotTag) {
			if err := p.forEachDirInManifest(ctx, m, files.RemoveFiles); err != nil {
				return 0, errors.Wrapf(err, "load manifest (no snapshot) %s", m.Path())
			}
		}
	}
	// Purge versioned SSTables
	if !oldest.IsZero() {
		nodeDir := RemoteSSTableBaseDir(anyM.ClusterID, anyM.DC, anyM.NodeID)
		opts := &scyllaclient.RcloneListDirOpts{
			FilesOnly:     true,
			Recurse:       true,
			VersionedOnly: true,
		}

		cb := func(item *scyllaclient.RcloneListDirItem) {
			ok, err := IsVersionedFileRemovable(oldest, item.Name)
			if err != nil {
				p.logger.Error(ctx, "Couldn't verify versioned file",
					"file", item.Name,
					"error", err,
				)
				return
			}
			if ok {
				fileDir := path.Join(nodeDir, path.Dir(item.Path))
				files.AddFiles(fileDir, []string{item.Name})

				p.logger.Info(ctx, "Found versioned SSTable to be removed",
					"dir", fileDir,
					"file", item.Name,
				)
			}
		}
		if err := p.client.RcloneListDirIter(ctx, p.host, anyM.Location.RemotePath(nodeDir), opts, cb); err != nil {
			return 0, errors.Wrapf(err, "find versioned SSTables")
		}
	}

	if _, err := p.deleteFiles(ctx, anyM.Location, files); err != nil {
		return 0, errors.Wrapf(err, "delete SSTables")
	}

	deletedManifests := 0
	for _, m := range manifests {
		if tags.Has(m.SnapshotTag) {
			// Note that schema files might not be backed up in the first place
			unsafePath := RemoteUnsafeSchemaFile(m.ClusterID, m.TaskID, m.SnapshotTag)
			if _, err := p.deleteFile(ctx, m.Location.RemotePath(unsafePath)); err != nil {
				p.logger.Info(ctx, "Remove unsafe schema file", "path", unsafePath, "error", err)
			}
			if _, err := p.deleteFile(ctx, m.Location.RemotePath(m.SchemaPath())); err != nil {
				p.logger.Info(ctx, "Remove schema file", "path", m.SchemaPath(), "error", err)
			}
			if _, err := p.deleteFile(ctx, m.Location.RemotePath(m.Path())); err != nil {
				p.logger.Info(ctx, "Failed to remove manifest", "path", m.Path(), "error", err)
			} else {
				deletedManifests++
			}
		}
	}

	return deletedManifests, nil
}

// ValidationResult is a summary generated by Validate.
type ValidationResult struct {
	ScannedFiles    int      `json:"scanned_files"`
	BrokenSnapshots []string `json:"broken_snapshots"`
	MissingFiles    int      `json:"missing_files"`
	OrphanedFiles   int      `json:"orphaned_files"`
	OrphanedBytes   int64    `json:"orphaned_bytes"`
	DeletedFiles    int      `json:"deleted_files"`
}

func (p purger) Validate(ctx context.Context, manifests []*ManifestInfo, deleteOrphanedFiles bool) (ValidationResult, error) {
	var result ValidationResult

	if len(manifests) == 0 {
		return result, nil
	}

	start := timeutc.Now()

	var (
		files             = make(fileSet)
		tempManifestFiles = make(fileSet)
		orphanedFiles     = make(fileSet)
	)

	for _, m := range manifests {
		var f func(dir string, files []string)
		if m.Temporary {
			f = tempManifestFiles.AddFiles
		} else {
			f = files.AddFiles
		}
		if err := p.forEachDirInManifest(ctx, m, f); err != nil {
			return result, errors.Wrapf(err, "load manifest (validate) %s", m.Path())
		}
	}

	handler := func(item *scyllaclient.RcloneListDirItem) {
		result.ScannedFiles++

		defer func() {
			if result.ScannedFiles%p.notifyEach == 0 {
				p.onScan(ctx, result)
			}
		}()

		// OK, file from manifest
		if files.Has(item.Path) {
			files.Remove(item.Path)
			return
		}
		// OK, file from temporary manifest i.e. running backup
		if tempManifestFiles.Has(item.Path) {
			tempManifestFiles.Remove(item.Path)
			return
		}
		// OK, file added after we started
		if time.Time(item.ModTime).After(start) {
			return
		}

		// NOK, handle orphaned file
		result.OrphanedFiles++
		result.OrphanedBytes += item.Size
		orphanedFiles.Add(item.Path)
	}
	if err := p.forEachRemoteFile(ctx, manifests[0], handler); err != nil {
		return result, errors.Wrap(err, "list files")
	}

	result.MissingFiles = files.Size()
	p.onScan(ctx, result)

	if result.MissingFiles > 0 {
		p.logger.Info(ctx, "Found missing files, looking for affected manifests")
		if bs, err := p.findBrokenSnapshots(ctx, manifests, files); err != nil {
			p.logger.Error(ctx, "Error while finding broken snapshots", "error", err)
		} else {
			result.BrokenSnapshots = bs
		}
	}

	// Remove orphaned files
	if deleteOrphanedFiles {
		n, err := p.deleteFiles(ctx, manifests[0].Location, orphanedFiles)
		if err != nil {
			return result, errors.Wrapf(err, "delete")
		}
		result.DeletedFiles = n
	}

	return result, nil
}

func (p purger) onScan(ctx context.Context, result ValidationResult) {
	p.logger.Info(ctx, "Scanning files",
		"scanned_files", result.ScannedFiles,
		"orphaned_files", result.OrphanedFiles,
		"orphaned_bytes", result.OrphanedBytes,
	)
	if p.OnScan != nil {
		p.OnScan(result.ScannedFiles, result.OrphanedFiles, result.OrphanedBytes)
	}
}

func (p purger) findBrokenSnapshots(ctx context.Context, manifests []*ManifestInfo, missingFiles fileSet) ([]string, error) {
	if missingFiles.Size() == 0 {
		return nil, nil
	}

	s := strset.New()

	for _, m := range manifests {
		if m.Temporary {
			continue
		}
		if err := p.forEachDirInManifest(ctx, m, func(dir string, files []string) {
			if missingFiles.HasAnyFiles(dir, files) {
				s.Add(m.SnapshotTag)
			}
		}); err != nil {
			// Ignore manifests removed while validate was running
			if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
				continue
			}
			return nil, errors.Wrapf(err, "load manifest (find broken) %s", m.Path())
		}
	}

	v := s.List()
	sort.Strings(v)
	return v, nil
}

func (p purger) forEachDirInManifest(ctx context.Context, m *ManifestInfo, callback func(dir string, files []string)) error {
	p.logger.Info(ctx, "Reading manifest",
		"task", m.TaskID,
		"snapshot_tag", m.SnapshotTag,
	)

	var c ManifestContentWithIndex

	r, err := p.client.RcloneOpen(ctx, p.host, m.Location.RemotePath(m.Path()))
	if err != nil {
		return err
	}
	defer r.Close()

	defer func() {
		if c.IP != "" {
			p.nodeIP[m.NodeID] = c.IP
		}
	}()

	if err := c.Read(r); err != nil {
		return err
	}

	return c.ForEachIndexIterFiles(nil, m, callback)
}

func (p purger) forEachRemoteFile(ctx context.Context, m *ManifestInfo, f func(*scyllaclient.RcloneListDirItem)) error {
	baseDir := RemoteSSTableBaseDir(m.ClusterID, m.DC, m.NodeID)
	wrapper := func(item *scyllaclient.RcloneListDirItem) {
		item.Path = path.Join(baseDir, item.Path)
		f(item)
	}
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly:   true,
		Recurse:     true,
		ShowModTime: true,
		NewestOnly:  true,
	}
	return p.client.RcloneListDirIter(ctx, p.host, m.Location.RemotePath(baseDir), &opts, wrapper)
}

func (p purger) deleteFiles(ctx context.Context, location Location, files fileSet) (int, error) {
	if files.Size() == 0 {
		return 0, nil
	}

	var (
		dirs    = files.Dirs()
		total   = files.Size()
		success atomic.Int64
		missing atomic.Int64
	)

	defer func() {
		s, m := int(success.Load()), int(missing.Load())
		p.onDelete(ctx, total, s, m)
	}()

	// We cap the nr. of dirs purged in parallel to avoid opening expressive TCP connections in case there is no idle connection to host.
	// maxParallelDirs is aligned with MaxIdleConnsPerHost in scyllaclient.DefaultTransport.
	const maxParallelDirs = 100

	f := func(i int) error {
		var (
			dir  = dirs[i]
			rerr error
		)

		files.DirSet(dir).Each(func(file string) bool {
			f := path.Join(dir, file)
			ok, err := p.deleteFile(ctx, location.RemotePath(f))
			// On error exit iteration and report error
			if err != nil {
				rerr = errors.Wrapf(err, "file %s", f)
				return false
			}
			s, m := int(success.Inc()), 0
			if !ok {
				m = int(missing.Inc())
			}
			if s%p.notifyEach == 0 {
				if m == 0 {
					m = int(missing.Load())
				}
				p.onDelete(ctx, total, s, m)
			}

			return true
		})

		return rerr
	}

	notify := func(i int, err error) {
		p.logger.Error(ctx, "Failed to delete files",
			"dir", dirs[i],
			"error", err,
		)
	}

	if err := parallel.Run(len(dirs), maxParallelDirs, f, notify); err != nil {
		return int(success.Load()), err
	}
	return int(success.Load()), nil
}

func (p purger) onDelete(ctx context.Context, total, success, missing int) {
	p.logger.Info(ctx, "Deleted files",
		"success", success,
		"missing", missing,
		"total", total,
	)
	if p.OnDelete != nil {
		p.OnDelete(total, success)
	}
}

func (p purger) deleteFile(ctx context.Context, path string) (bool, error) {
	p.logger.Debug(ctx, "Deleting file", "path", path)
	err := p.client.RcloneDeleteFile(ctx, p.host, path)
	if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
		return false, nil
	}
	return true, err
}

// Host can be called from OnPreDelete and OnDelete callbacks to convert node ID
// to IP for metrics purposes.
func (p purger) Host(nodeID string) string {
	return p.nodeIP[nodeID]
}
