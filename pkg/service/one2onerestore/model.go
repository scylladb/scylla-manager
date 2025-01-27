// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []Location    `json:"location"`
	Keyspace        []string      `json:"keyspace,omitempty"`
	SourceClusterID uuid.UUID     `json:"source_cluster_id"`
	SnapshotTag     string        `json:"snapshot_tag"`
	NodesMapping    []nodeMapping `json:"nodes_mapping"`
}

func defaultTarget() Target {
	return Target{}
}

type nodeMapping struct {
	Source node `json:"source"`
	Target node `json:"target"`
}

type node struct {
	DC     string `json:"dc"`
	Rack   string `json:"rack"`
	HostID string `json:"host_id"`
}

// LocationInfo contains some basic information about Location
// Intended to be used for simplifying access to the Location.
type LocationInfo struct {
	Location Location
	// Hosts that have an access to the Location
	Hosts []Host
	// Manifests from the Location
	Manifest []*ManifestInfo
}

// Host contains basic information about Scylla node.
type Host struct {
	ID   string
	DC   string
	Addr string
}

func (t *Target) validateProperties() error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if !IsSnapshotTag(t.SnapshotTag) {
		return errors.Errorf("unexpected snapshot-tag format: %s", t.SnapshotTag)
	}
	if t.SourceClusterID == uuid.Nil {
		return errors.New("source cluster id is empty")
	}
	if len(t.NodesMapping) == 0 {
		return errors.New("nodes mapping is empty")
	}

	return nil
}
