// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"go.uber.org/multierr"
)

type worker struct {
	managerSession gocqlx.Session

	client         *scyllaclient.Client
	clusterSession gocqlx.Session

	logger log.Logger
}

// restore is an actual 1-1-restore stages.
func (w *worker) restore(ctx context.Context, workload []hostWorkload, target Target) (err error) {
	views, err := w.dropViews(ctx, workload, target.Keyspace)
	if err != nil {
		return errors.Wrap(err, "drop views")
	}
	defer func() {
		if rErr := w.reCreateViews(ctx, views); rErr != nil {
			err = multierr.Combine(
				err,
				errors.Wrap(rErr, "recreate views"),
			)
		}
	}()

	return w.restoreTables(ctx, workload, target.Keyspace)
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

// prepareHostWorkload is a helper function that creates a hostWorkload structure convenient for use in later 1-1-restore stages.
// This avoids the need to repeat operations like node mapping and fetching manifest content.
func (w *worker) prepareHostWorkload(ctx context.Context, manifests []*backupspec.ManifestInfo, hosts []Host, nodeMappings []nodeMapping) ([]hostWorkload, error) {
	targetBySourceHostID, err := mapTargetHostToSource(hosts, nodeMappings)
	if err != nil {
		return nil, errors.Wrap(err, "invalid node mapping")
	}

	result := make([]hostWorkload, len(manifests))
	return result, parallel.Run(len(manifests), len(manifests), func(i int) error {
		m := manifests[i]
		h := targetBySourceHostID[m.NodeID]

		mc, err := w.getManifestContent(ctx, h.Addr, m)
		if err != nil {
			return errors.Wrap(err, "manifest content")
		}
		result[i] = hostWorkload{
			host:            h,
			manifestInfo:    m,
			manifestContent: mc,
		}

		return nil
	}, parallel.NopNotify)
}

// alterSchemaRetryWrapper is useful when executing many statements altering schema,
// as it might take more time for Scylla to process them one after another.
// This wrapper exits on: success, context cancel, op returned non-timeout error or after maxTotalTime has passed.
func alterSchemaRetryWrapper(ctx context.Context, op func() error, notify func(err error, wait time.Duration)) error {
	const (
		minWait      = 5 * time.Second
		maxWait      = 1 * time.Minute
		maxTotalTime = 15 * time.Minute
		multiplier   = 2
		jitter       = 0.2
	)
	backoff := retry.NewExponentialBackoff(minWait, maxTotalTime, maxWait, multiplier, jitter)

	wrappedOp := func() error {
		err := op()
		if err == nil || strings.Contains(err.Error(), "timeout") {
			return err
		}
		// All non-timeout errors shouldn't be retried
		return retry.Permanent(err)
	}

	return retry.WithNotify(ctx, wrappedOp, backoff, notify)
}
