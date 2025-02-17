// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

type worker struct {
	managerSession gocqlx.Session

	client         *scyllaclient.Client
	clusterSession gocqlx.Session

	logger log.Logger
}

// getAllSnapshotManifestsAndTargetHosts gets backup(source) cluster node represented by manifests and target cluster nodes.
func (w *worker) getAllSnapshotManifestsAndTargetHosts(ctx context.Context, target Target) ([]*backupspec.ManifestInfo, []Host, error) {
	nodeStatus, err := w.client.Status(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "nodes status")
	}
	var allManifests []*backupspec.ManifestInfo
	for _, location := range target.Location {
		nodeAddr, err := findNodeFromDC(nodeStatus, location.DC, target.NodesMapping)
		if err != nil {
			return nil, nil, errors.Wrap(err, "invalid mappings")
		}
		manifests, err := w.getManifestInfo(ctx, nodeAddr, target.SnapshotTag, target.SourceClusterID, location)
		if err != nil {
			return nil, nil, err
		}
		allManifests = append(allManifests, manifests...)
	}
	return allManifests, nodesToHosts(nodeStatus), nil
}

func findNodeFromDC(nodeStatus scyllaclient.NodeStatusInfoSlice, locationDC string, nodeMappings []nodeMapping) (addr string, err error) {
	// When location DC is empty, it means that location contains all backup DCs
	// and should be accessible by any node from target cluster.
	if locationDC == "" {
		return nodeStatus[0].Addr, nil
	}
	// Otherwise find node from location dc accordingly to node mappings
	var targetDC string
	for _, nodeMap := range nodeMappings {
		if nodeMap.Source.DC == locationDC {
			targetDC = nodeMap.Target.DC
			break
		}
	}
	if targetDC == "" {
		return "", errors.Errorf("mapping for source DC is not found: %s", locationDC)
	}
	for _, node := range nodeStatus {
		if node.Datacenter != targetDC {
			continue
		}
		return node.Addr, nil
	}
	return "", errors.Errorf("node with access to location dc is not found: %s", locationDC)
}

func nodesToHosts(nodes scyllaclient.NodeStatusInfoSlice) []Host {
	var hosts []Host
	for _, n := range nodes {
		hosts = append(hosts, Host{
			ID:   n.HostID,
			DC:   n.Datacenter,
			Addr: n.Addr,
		})
	}
	return hosts
}
