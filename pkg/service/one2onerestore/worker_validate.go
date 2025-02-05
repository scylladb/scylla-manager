// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"golang.org/x/sync/errgroup"
)

// validateClusters checks if 1-1-restore can be performed on provided clusters.
func (w *worker) validateClusters(ctx context.Context, manifests []*backupspec.ManifestInfo, hosts []Host, nodeMappings []nodeMapping) error {
	if err := w.client.VerifyNodesAvailability(ctx); err != nil {
		return errors.Wrap(err, "all nodes must be available")
	}

	if len(nodeMappings) != len(hosts) {
		return errors.Errorf("nodes count (%d) != node count in mappings (%d)", len(hosts), len(nodeMappings))
	}

	sourceNodeInfo, targetNodeInfo, err := w.collectNodeValidationInfo(ctx, manifests, hosts, nodeMappings)
	if err != nil {
		return errors.Wrap(err, "collect nodes info")
	}

	if err := checkOne2OneRestoreCompatibility(sourceNodeInfo, targetNodeInfo, nodeMappings); err != nil {
		return errors.Wrap(err, "clusters not equal")
	}

	return nil
}

func checkOne2OneRestoreCompatibility(sourceNodeInfo, targetNodeInfo []nodeValidationInfo, nodeMappings []nodeMapping) error {
	if len(sourceNodeInfo) != len(targetNodeInfo) {
		return errors.Errorf("clusters have different nodes count: source %d != target %d", len(sourceNodeInfo), len(targetNodeInfo))
	}

	mappedSourceNodeInfo, err := mapSourceNodesToTarget(sourceNodeInfo, nodeMappings)
	if err != nil {
		return errors.Wrap(err, "invalid node mappings")
	}

	for _, target := range targetNodeInfo {
		source, ok := mappedSourceNodeInfo[node{DC: target.DC, Rack: target.Rack, HostID: target.HostID}]
		if !ok {
			return errors.Errorf("target node has no match in source cluster:%s %s %s", target.DC, target.Rack, target.HostID)
		}
		if source.ShardCount != target.ShardCount {
			return errors.Errorf("source ShardCount doesn't match target ShardCount")
		}
		if source.StorageSize > target.StorageSize {
			return errors.Errorf("source StorageSize greater than target StorageSize")
		}
		if !slices.Equal(source.Tokens, target.Tokens) {
			return errors.Errorf("source Tokens doesn't match target Tokens")
		}
	}
	return nil
}

func mapTargetHostToSource(targetHosts []Host, nodeMappings []nodeMapping) (map[string]Host, error) {
	sourceByTargetHostID := map[string]string{}
	for _, mapping := range nodeMappings {
		sourceByTargetHostID[mapping.Target.HostID] = mapping.Source.HostID
	}

	result := map[string]Host{}
	for _, host := range targetHosts {
		sourceHostID, ok := sourceByTargetHostID[host.ID]
		if !ok {
			return nil, errors.Errorf("mapping for target node (%s) is not found", host.ID)
		}
		result[sourceHostID] = host
	}
	return result, nil
}

func mapSourceNodesToTarget(sourceNodeInfo []nodeValidationInfo, nodeMappings []nodeMapping) (map[node]nodeValidationInfo, error) {
	sourceMappings := map[node]node{}
	for _, m := range nodeMappings {
		sourceMappings[m.Source] = m.Target
	}

	sourceByTarget := map[node]nodeValidationInfo{}
	for _, source := range sourceNodeInfo {
		target, ok := sourceMappings[node{DC: source.DC, Rack: source.Rack, HostID: source.HostID}]
		if !ok {
			return nil, errors.Errorf("mapping for source node (%v) is not found", source)
		}
		sourceByTarget[target] = source
	}
	return sourceByTarget, nil
}

func (w *worker) collectNodeValidationInfo(
	ctx context.Context,
	manifests []*backupspec.ManifestInfo,
	hosts []Host,
	nodeMappings []nodeMapping,
) (sourceCluster, targetCluster []nodeValidationInfo, err error) {
	targetHostBySourceID, err := mapTargetHostToSource(hosts, nodeMappings)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid node mappings")
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		sourceInfo, err := w.getSourceClusterNodeInfo(ctx, manifests, targetHostBySourceID)
		if err != nil {
			return errors.Wrap(err, "source cluster node info")
		}

		sourceCluster = sourceInfo
		return nil
	})
	g.Go(func() error {
		targetInfo, err := w.getTargetClusterNodeInfo(ctx, hosts)
		if err != nil {
			return errors.Wrap(err, "target cluster node info")
		}

		targetCluster = targetInfo
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return sourceCluster, targetCluster, nil
}

type nodeValidationInfo struct {
	DC          string
	Rack        string
	HostID      string
	ShardCount  int
	StorageSize uint64
	Tokens      []int64
}

func (w *worker) getTargetClusterNodeInfo(ctx context.Context, hosts []Host) ([]nodeValidationInfo, error) {
	result := make([]nodeValidationInfo, len(hosts))
	err := parallel.Run(len(hosts), parallel.NoLimit, func(i int) error {
		h := hosts[i]

		ni, err := w.client.NodeInfo(ctx, h.Addr)
		if err != nil {
			return errors.Wrap(err, "get node info")
		}
		rack, err := w.client.HostRack(ctx, h.Addr)
		if err != nil {
			return errors.Wrap(err, "get node rack")
		}
		tokens, err := w.client.Tokens(ctx, h.Addr)
		if err != nil {
			return errors.Wrap(err, "get node tokens")
		}
		shardCount, err := w.client.ShardCount(ctx, h.Addr)
		if err != nil {
			return errors.Wrap(err, "get shard count")
		}

		// Make sure tokens are sorted,
		// so we can compare them later.
		slices.Sort(tokens)

		result[i] = nodeValidationInfo{
			HostID:      h.ID,
			DC:          h.DC,
			Rack:        rack,
			ShardCount:  int(shardCount),
			StorageSize: ni.StorageSize,
			Tokens:      tokens,
		}

		return nil
	}, parallel.NopNotify)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (w *worker) getSourceClusterNodeInfo(ctx context.Context, manifests []*backupspec.ManifestInfo, targetHostBySourceID map[string]Host) ([]nodeValidationInfo, error) {
	result := make([]nodeValidationInfo, len(manifests))
	err := parallel.Run(len(manifests), parallel.NoLimit, func(i int) error {
		m := manifests[i]
		h := targetHostBySourceID[m.NodeID]
		mc, err := w.getManifestContent(ctx, h.Addr, m)
		if err != nil {
			return err
		}
		// Make sure tokens are sorted,
		// so we can compare them later.
		slices.Sort(mc.Tokens)

		result[i] = nodeValidationInfo{
			DC:          mc.DC,
			Rack:        mc.Rack,
			HostID:      mc.NodeID,
			ShardCount:  mc.ShardCount,
			StorageSize: mc.StorageSize,
			Tokens:      mc.Tokens,
		}
		return nil
	}, parallel.NopNotify)
	if err != nil {
		return nil, err
	}
	return result, nil
}
