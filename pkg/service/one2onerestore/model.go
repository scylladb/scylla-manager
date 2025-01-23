// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []Location    `json:"location"`
	Keyspace        []string      `json:"keyspace,omitempty"`
	SourceClusterID string        `json:"source_cluster_id"`
	SnapshotTag     string        `json:"snapshot_tag"`
	NodesMapping    []nodeMapping `json:"nodes_mapping"`
	BatchSize       int           `json:"batch_size,omitempty"`
	Parallel        int           `json:"parallel,omitempty"`
	Transfers       int           `json:"transfers"`
	RateLimit       []DCLimit     `json:"rate_limit,omitempty"`
	AllowCompaction bool          `json:"allow_compaction,omitempty"`
	UnpinAgentCPU   bool          `json:"unpin_agent_cpu"`
	Continue        bool          `json:"continue"`
}

func defaultTarget() Target {
	return Target{
		BatchSize: 2,
		Parallel:  0,
		Transfers: 0,
		Continue:  true,
	}
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

func (t *Target) validateProperties() error {
	if len(t.Location) == 0 {
		return errors.New("missing location")
	}
	if _, err := SnapshotTagTime(t.SnapshotTag); err != nil {
		return err
	}
	if t.BatchSize < 0 {
		return errors.New("batch size param has to be greater or equal to zero")
	}
	if t.Parallel < 0 {
		return errors.New("parallel param has to be greater or equal to zero")
	}
	if t.Transfers != scyllaclient.TransfersFromConfig && t.Transfers != 0 && t.Transfers < 1 {
		return errors.New("transfers param has to be equal to -1 (set transfers to the value from scylla-manager-agent.yaml config) " +
			"or 0 (set transfers for fastest download) or greater than zero")
	}
	if t.SourceClusterID == "" {
		return errors.New("source cluster id is empty")
	}
	if len(t.NodesMapping) == 0 {
		return errors.New("nodes mapping is empty")
	}

	return nil
}
