// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
	"github.com/scylladb/go-set/strset"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ManifestLookupCriteria specifies which manifest you want to use to download
// files.
type ManifestLookupCriteria struct {
	NodeID      uuid.UUID `json:"node_id"`
	SnapshotTag string    `json:"snapshot_tag"`
}

func (c ManifestLookupCriteria) matches(m *backup.RemoteManifest) bool {
	if c.NodeID != uuid.Nil && c.NodeID.String() != m.NodeID {
		return false
	}
	if c.SnapshotTag != "" && c.SnapshotTag != m.SnapshotTag {
		return false
	}
	return true
}

// LookupManifest finds and loads manifest base on the lookup criteria.
func (d *Downloader) LookupManifest(ctx context.Context, c ManifestLookupCriteria) (*backup.RemoteManifest, error) {
	d.logger.Info(ctx, "Searching for manifest", "criteria", c)

	if c.NodeID == uuid.Nil {
		return nil, errors.New("invalid criteria: missing node ID")
	}
	if c.SnapshotTag == "" {
		return nil, errors.New("invalid criteria: missing snapshot tag")
	}

	var (
		m  = new(backup.RemoteManifest)
		ok bool
	)

	lookup := func(o fs.Object) error {
		if ok {
			return nil
		}

		if err := m.ParsePath(o.String()); err != nil {
			return nil
		}
		if m.Temporary {
			return nil
		}
		if c.matches(m) {
			if err := readManifestContentFromObject(ctx, m, o); err != nil {
				return errors.Wrap(err, "read content")
			}
			ok = true
		}
		return nil
	}
	if err := d.forEachMetaDirObject(ctx, lookup); err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no manifests found")
	}

	return m, nil
}

// ListNodeSnapshots returns list of snapshots for a given node.
func (d *Downloader) ListNodeSnapshots(ctx context.Context, nodeID uuid.UUID) ([]string, error) {
	d.logger.Info(ctx, "Searching for node snapshots", "node_id", nodeID)

	if nodeID == uuid.Nil {
		return nil, errors.New("invalid criteria: missing node ID")
	}

	var (
		snapshotTags []string

		m = new(backup.RemoteManifest)
		c = ManifestLookupCriteria{NodeID: nodeID}
	)

	lookup := func(o fs.Object) error {
		if err := m.ParsePath(o.String()); err != nil {
			return nil
		}
		if m.Temporary {
			return nil
		}
		if !c.matches(m) {
			return nil
		}
		if d.keyspace != nil {
			if err := readManifestContentFromObject(ctx, m, o); err != nil {
				return errors.Wrap(err, "read content")
			}
			if len(d.filteredIndex(ctx, m)) == 0 {
				return nil
			}
		}
		snapshotTags = append(snapshotTags, m.SnapshotTag)
		return nil
	}
	if err := d.forEachMetaDirObject(ctx, lookup); err != nil {
		return nil, err
	}

	sort.Slice(snapshotTags, func(i, j int) bool {
		return snapshotTags[i] > snapshotTags[j]
	})

	return snapshotTags, nil
}

// NodeInfo contains selected information on node from manifest.
type NodeInfo struct {
	ClusterID   uuid.UUID
	ClusterName string
	DC          string
	NodeID      string
	IP          string
}

// NodeInfoSlice adds WriteTo functionality to the slice.
type NodeInfoSlice []NodeInfo

func (s NodeInfoSlice) WriteTo(w io.Writer) (n int64, err error) {
	b := &strings.Builder{}
	defer func() {
		if err == nil {
			m, e := w.Write([]byte(b.String()))
			n, err = int64(m), e
		}
	}()

	var (
		lastClusterID uuid.UUID
		lastDC        string
	)
	for i, n := range s {
		if n.ClusterID != lastClusterID {
			if i > 0 {
				fmt.Fprintln(b)
			}
			if n.ClusterName == "" {
				fmt.Fprintf(b, "Cluster: %s\n", n.ClusterID)
			} else {
				fmt.Fprintf(b, "Cluster: %s (%s)\n", n.ClusterName, n.ClusterID)
			}
			lastClusterID = n.ClusterID
			lastDC = ""
		}
		if n.DC != lastDC {
			fmt.Fprintf(b, "%s:\n", n.DC)
			lastDC = n.DC
		}
		fmt.Fprintf(b, "  - %s (%s)\n", n.IP, n.NodeID)
	}
	return
}

// ListNodes returns a listing of all available nodes.
// Having that one can get a snapshot listing from ListSnapshots.
func (d *Downloader) ListNodes(ctx context.Context) (NodeInfoSlice, error) {
	d.logger.Info(ctx, "Listing nodes")

	var (
		nodes []NodeInfo

		m   = new(backup.RemoteManifest)
		ids = strset.New()
	)

	lookup := func(o fs.Object) error {
		if err := m.ParsePath(o.String()); err != nil {
			return nil
		}
		if m.Temporary {
			return nil
		}
		if ids.Has(m.NodeID) {
			return nil
		}
		if err := readManifestContentFromObject(ctx, m, o); err != nil {
			return errors.Wrap(err, "read content")
		}
		nodes = append(nodes, NodeInfo{
			ClusterID:   m.ClusterID,
			ClusterName: m.Content.ClusterName,
			DC:          m.DC,
			NodeID:      m.NodeID,
			IP:          m.Content.IP,
		})
		ids.Add(m.NodeID)
		return nil
	}
	if err := d.forEachMetaDirObject(ctx, lookup); err != nil {
		return nil, err
	}

	sort.Slice(nodes, func(i, j int) bool {
		if r := strings.Compare(nodes[i].ClusterID.String(), nodes[j].ClusterID.String()); r != 0 {
			return r < 0
		}
		if r := strings.Compare(nodes[i].DC, nodes[j].DC); r != 0 {
			return r < 0
		}
		if r := strings.Compare(nodes[i].IP, nodes[j].IP); r != 0 {
			return r < 0
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})

	return nodes, nil
}

func (d *Downloader) forEachMetaDirObject(ctx context.Context, fn func(o fs.Object) error) error {
	baseDir := path.Join("backup", string(backup.MetaDirKind))
	return walk.ListR(ctx, d.fsrc, baseDir, true, backup.RemoteManifestLevel(baseDir)+1, walk.ListObjects, func(e fs.DirEntries) error { return e.ForObjectError(fn) })
}

func readManifestContentFromObject(ctx context.Context, m *backup.RemoteManifest, o fs.Object) error {
	r, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close()
	return m.ReadContent(r)
}
