// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
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

// getManifestsAndHosts checks that each host in target cluster should have an access to at least one target location and fetches manifest info.
func (w *worker) getManifestsAndHosts(ctx context.Context, target Target) ([]*backupspec.ManifestInfo, []Host, error) {
	nodeStatus, err := w.client.Status(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "nodes status")
	}
	var (
		allManifests  []*backupspec.ManifestInfo
		nodesCountSet = strset.New()
	)
	for _, location := range target.Location {
		// Ignore location.DC because all mappings should be specified via nodes-mapping file
		nodes, err := w.getNodesWithAccess(ctx, nodeStatus, location.RemotePath(""))
		if err != nil {
			return nil, nil, err
		}
		manifests, err := w.getManifestInfo(ctx, nodes[0].Addr, target.SnapshotTag, target.SourceClusterID, location)
		if err != nil {
			return nil, nil, err
		}

		allManifests = append(allManifests, manifests...)

		for _, n := range nodes {
			nodesCountSet.Add(n.HostID)
		}
	}

	// If manifest count != nodes with access count means that 1-1 restore is not possible.
	if len(allManifests) != nodesCountSet.Size() || len(allManifests) != len(nodeStatus) {
		return nil, nil, fmt.Errorf("manifest count (%d) != target nodes (%d)", len(allManifests), nodesCountSet.Size())
	}

	return allManifests, nodesToHosts(nodeStatus), nil
}

func (w *worker) getNodesWithAccess(ctx context.Context, nodesInfo scyllaclient.NodeStatusInfoSlice, remotePath string) (scyllaclient.NodeStatusInfoSlice, error) {
	nodesWithAccess, err := w.client.GetNodesWithLocationAccess(ctx, nodesInfo, remotePath)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, errors.Errorf("specified bucket does not exist: %s", remotePath)
		}
		return nil, errors.Wrapf(err, "location %s is not accessible", remotePath)
	}
	if len(nodesWithAccess) == 0 {
		return nil, errors.Errorf("no nodes with location %s access", remotePath)
	}
	return nodesWithAccess, nil
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
