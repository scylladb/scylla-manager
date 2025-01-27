// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

type worker struct {
	managerSession gocqlx.Session

	client         *scyllaclient.Client
	clusterSession gocqlx.Session

	logger log.Logger
}

func getSourceMappings(mappings []nodeMapping) map[node]node {
	sourceMappings := map[node]node{}
	for _, m := range mappings {
		sourceMappings[m.Source] = m.Target
	}
	return sourceMappings
}

func (w *worker) getLocationInfo(ctx context.Context, target Target) ([]LocationInfo, error) {
	var result []LocationInfo

	nodeStatus, err := w.client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "nodes status")
	}
	for _, location := range target.Location {
		// Ignore location.DC because all mappings should be specified via nodes-mapping file
		nodes, err := w.getNodesWithAccess(ctx, nodeStatus, location.RemotePath(""))
		if err != nil {
			return nil, err
		}
		manifests, err := w.getManifestInfo(ctx, nodes[0].Addr, target.SnapshotTag, location)
		if err != nil {
			return nil, err
		}
		result = append(result, LocationInfo{
			Manifest: manifests,
			Location: location,
			Hosts:    nodesToHosts(nodes),
		})
	}

	return result, nil
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
