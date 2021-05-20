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
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/atomic"
)

// listAllManifests returns manifests for all nodes of a given cluster.
func listAllManifests(ctx context.Context, client *scyllaclient.Client, host string, location Location, clusterID uuid.UUID) ([]*RemoteManifest, error) {
	baseDir := RemoteMetaClusterDCDir(clusterID)
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}
	items, err := client.RcloneListDir(ctx, host, location.RemotePath(baseDir), &opts)
	if err != nil {
		return nil, err
	}

	var manifests []*RemoteManifest
	for _, item := range items {
		p := path.Join(baseDir, item.Path)
		m := &RemoteManifest{}
		if err := m.ParsePartialPath(p); err != nil {
			continue
		}
		m.Location = location
		manifests = append(manifests, m)
	}
	return manifests, nil
}

func groupManifestsByNode(manifests []*RemoteManifest) map[string][]*RemoteManifest {
	v := map[string][]*RemoteManifest{}
	for _, m := range manifests {
		v[m.NodeID] = append(v[m.NodeID], m)
	}
	return v
}

func groupManifestsByTask(manifests []*RemoteManifest) map[uuid.UUID][]*RemoteManifest {
	v := map[uuid.UUID][]*RemoteManifest{}
	for _, m := range manifests {
		v[m.TaskID] = append(v[m.TaskID], m)
	}
	return v
}

// staleTags returns collection of snapshot tags for manifests that are "stale".
// That is:
// - temporary manifests,
// - manifests over task retention policy,
// - manifests older than threshold if retention policy is unknown.
func staleTags(manifests []*RemoteManifest, policy map[uuid.UUID]int, threshold time.Time) *strset.Set {
	tags := strset.New()

	for taskID, taskManifests := range groupManifestsByTask(manifests) {
		taskPolicy := policy[taskID]
		taskTags := strset.New()
		for _, m := range taskManifests {
			switch {
			case m.Temporary:
				tags.Add(m.SnapshotTag)
			case taskPolicy > 0:
				taskTags.Add(m.SnapshotTag)
			default:
				t, _ := SnapshotTagTime(m.SnapshotTag) // nolint: errcheck
				if t.Before(threshold) {
					tags.Add(m.SnapshotTag)
				}
			}
		}
		if taskPolicy > 0 && taskTags.Size()-taskPolicy > 0 {
			l := taskTags.List()
			sort.Strings(l)
			tags.Add(l[:len(l)-taskPolicy]...)
		}
	}
	return tags
}

type purger struct {
	client *scyllaclient.Client
	host   string
	logger log.Logger
	// nodeIP maps node ID to IP based on information from read manifests.
	nodeIP map[string]string

	OnPreDelete func(files int)
	OnDelete    func()
}

func newPurger(client *scyllaclient.Client, host string, logger log.Logger) purger {
	return purger{
		client: client,
		host:   host,
		logger: logger,
		nodeIP: make(map[string]string),
	}
}

func (p purger) PurgeSnapshotTags(ctx context.Context, manifests []*RemoteManifest, tags *strset.Set) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}

	staleManifests := 0
	files := make(fileSet)
	c := new(ManifestContent)

	for _, m := range manifests {
		if tags.Has(m.SnapshotTag) {
			staleManifests++
			p.logger.Info(ctx, "Found manifest to remove",
				"host", p.host,
				"task", m.TaskID,
				"snapshot_tag", m.SnapshotTag,
				"node", m.NodeID,
				"temporary", m.Temporary,
			)
			if err := p.loadManifestContentInto(ctx, m, c); err != nil {
				return 0, errors.Wrapf(err, "load manifest %s", m.RemoteManifestFile())
			}
			p.forEachFile(m, c, files.Add)
		}
	}
	if staleManifests == 0 {
		return 0, nil
	}
	for _, m := range manifests {
		if !tags.Has(m.SnapshotTag) {
			if err := p.loadManifestContentInto(ctx, m, c); err != nil {
				return 0, errors.Wrapf(err, "load manifest %s", m.RemoteManifestFile())
			}
			p.forEachFile(m, c, files.Remove)
		}
	}
	if err := p.deleteFiles(ctx, manifests[0].Location, files); err != nil {
		return 0, errors.Wrapf(err, "delete")
	}
	deletedManifests := 0
	for _, m := range manifests {
		if tags.Has(m.SnapshotTag) {
			if _, err := p.deleteFile(ctx, m.Location.RemotePath(m.RemoteSchemaFile())); err != nil {
				p.logger.Info(ctx, "Failed to remove schema file", "path", m.RemoteSchemaFile(), "error", err)
			}
			if _, err := p.deleteFile(ctx, m.Location.RemotePath(m.RemoteManifestFile())); err != nil {
				p.logger.Info(ctx, "Failed to remove manifest", "path", m.RemoteManifestFile(), "error", err)
			} else {
				deletedManifests++
			}
		}
	}
	return deletedManifests, nil
}

func (p purger) loadManifestContentInto(ctx context.Context, m *RemoteManifest, c *ManifestContent) error {
	*c = ManifestContent{}
	r, err := p.client.RcloneOpen(ctx, p.host, m.Location.RemotePath(m.RemoteManifestFile()))
	if err != nil {
		return err
	}
	defer r.Close()

	defer func() {
		if c.IP != "" {
			p.nodeIP[m.NodeID] = c.IP
		}
	}()

	return c.Read(r)
}

func (p purger) forEachFile(m *RemoteManifest, c *ManifestContent, callback func(path string)) {
	baseDir := RemoteSSTableBaseDir(m.ClusterID, m.DC, m.NodeID)
	for _, fi := range c.Index {
		for _, f := range fi.Files {
			callback(path.Join(baseDir, ssTablePathWithKeyspacePrefix(fi.Keyspace, fi.Table, fi.Version, f)))
		}
	}
}

func (p purger) deleteFiles(ctx context.Context, location Location, files fileSet) error {
	const logEach = 100

	var (
		dirs    = files.Dirs()
		total   = files.Size()
		success atomic.Int64
		missing atomic.Int64
	)

	if p.OnPreDelete != nil {
		p.OnPreDelete(total)
	}

	parallel.Run(len(dirs), parallel.NoLimit, func(i int) error { // nolint: errcheck
		dir := dirs[i]

		files.DirSet(dir).Each(func(item string) bool {
			f := path.Join(dir, item)
			ok, err := p.deleteFile(ctx, location.RemotePath(f))
			if err != nil {
				p.logger.Info(ctx, "Delete file error", "file", f, "error", err)
				return false
			}
			if !ok {
				missing.Inc()
			} else {
				success.Inc()
			}
			if p.OnDelete != nil {
				p.OnDelete()
			}

			s, m := success.Load(), missing.Load()
			if s+m >= logEach && (s+m)%logEach == 0 {
				p.logger.Info(ctx, "Deleted files",
					"host", p.host,
					"success", s,
					"missing", m,
					"total", total,
				)
			}
			return true
		})

		return nil
	})

	s, m := success.Load(), missing.Load()
	p.logger.Info(ctx, "Deleted files",
		"host", p.host,
		"success", s,
		"missing", m,
		"total", total,
	)
	if s+m != int64(total) {
		return errors.Errorf("some files were not deleted, removed %d out of %d, see logs for more details", s, total)
	}

	return nil
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
