// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Target specifies what data should be restored and from which locations.
type Target struct {
	Location        []backupspec.Location `json:"location"`
	Keyspace        []string              `json:"keyspace,omitempty"`
	SourceClusterID uuid.UUID             `json:"source_cluster_id"`
	SnapshotTag     string                `json:"snapshot_tag"`
	NodesMapping    []nodeMapping         `json:"nodes_mapping"`
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
	if !backupspec.IsSnapshotTag(t.SnapshotTag) {
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

type dcRack struct {
	dc   string
	rack string
}

func validateNodesMapping(nodesMapping []nodeMapping) error {
	if len(nodesMapping) == 0 {
		return errors.New("empty")
	}

	var (
		sourceDCMap = map[dcRack]dcRack{}
		targetDCMap = map[dcRack]dcRack{}

		sourceDCRackMap = map[dcRack]dcRack{}
		targetDCRackMap = map[dcRack]dcRack{}

		sourceNodes = map[string]struct{}{}
		targetNodes = map[string]struct{}{}
	)

	for _, nodeMapping := range nodesMapping {
		s, t := nodeMapping.Source, nodeMapping.Target

		// Check DCs
		if err := checkDCRackMapping(sourceDCMap, dcRack{dc: s.DC}, dcRack{dc: t.DC}); err != nil {
			return err
		}
		if err := checkDCRackMapping(targetDCMap, dcRack{dc: t.DC}, dcRack{dc: s.DC}); err != nil {
			return err
		}
		// Check Racks
		sourceDCRack, targetDCRack := dcRack{dc: s.DC, rack: s.Rack}, dcRack{dc: t.DC, rack: t.Rack}
		if err := checkDCRackMapping(sourceDCRackMap, sourceDCRack, targetDCRack); err != nil {
			return err
		}
		if err := checkDCRackMapping(targetDCRackMap, targetDCRack, sourceDCRack); err != nil {
			return err
		}
		// Check Hosts
		if err := checkHostMapping(sourceNodes, s.HostID); err != nil {
			return err
		}
		if err := checkHostMapping(targetNodes, t.HostID); err != nil {
			return err
		}
	}
	return nil
}

func checkDCRackMapping(dcRackMap map[dcRack]dcRack, source, target dcRack) error {
	mapped, ok := dcRackMap[source]
	if !ok {
		dcRackMap[source] = target
		return nil
	}
	if mapped != target {
		return errors.Errorf("%s %s is already mapped to %s %s", source.dc, source.rack, mapped.dc, mapped.rack)
	}
	return nil
}

func checkHostMapping(hostMap map[string]struct{}, hostID string) error {
	if _, ok := hostMap[hostID]; !ok {
		hostMap[hostID] = struct{}{}
		return nil
	}
	return errors.Errorf("host is already mapped: %s", hostID)
}
