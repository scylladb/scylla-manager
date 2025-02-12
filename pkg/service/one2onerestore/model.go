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
	if err := validateNodesMapping(t.NodesMapping); err != nil {
		return errors.Wrap(err, "nodes mapping")
	}
	return nil
}

func validateNodesMapping(nodesMapping []nodeMapping) error {
	if len(nodesMapping) == 0 {
		return errors.New("empty")
	}

	var (
		sourceDCCount = map[string]int{}
		targetDCCount = map[string]int{}

		sourceRackCount = map[string]int{}
		targetRackCount = map[string]int{}
	)

	for _, nodeMapping := range nodesMapping {
		s, t := nodeMapping.Source, nodeMapping.Target

		sourceDCCount[s.DC]++
		targetDCCount[t.DC]++

		sourceRackCount[s.DC+s.Rack]++
		targetRackCount[t.DC+t.Rack]++

		if sourceDCCount[s.DC] != targetDCCount[t.DC] {
			return errors.Errorf("source and target clusters has different number of nodes per DC")
		}

		if sourceRackCount[s.DC+s.Rack] != targetRackCount[t.DC+t.Rack] {
			return errors.Errorf("source and target clusters has different number of nodes per Rack")
		}
	}
	return nil
}
