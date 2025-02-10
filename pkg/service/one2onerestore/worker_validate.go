// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

// validateClusters checks if 1-1-restore can be performed on provided clusters.
func (w *worker) validateClusters(ctx context.Context, manifests []*backupspec.ManifestInfo, hosts []Host, nodeMappings []nodeMapping) error {
	if len(manifests) != len(hosts) {
		return errors.Errorf("different node count in source and target clusters: %d != %d", len(manifests), len(hosts))
	}

	if err := w.client.VerifyNodesAvailability(ctx); err != nil {
		return errors.Wrap(err, "all nodes must be available")
	}

	targetHostBySourceID, err := mapTargetHostToSource(hosts, nodeMappings)
	if err != nil {
		return errors.Wrap(err, "invalid node mappings")
	}

	return parallel.Run(len(manifests), parallel.NoLimit, func(i int) error {
		m := manifests[i]
		h := targetHostBySourceID[m.NodeID]

		targetNodeInfo, err := w.getTargetNodeInfo(ctx, h)
		if err != nil {
			return errors.Wrap(err, "target cluster node info")
		}

		sourceNodeInfo, err := w.getSourceNodeInfo(ctx, h, m)
		if err != nil {
			return errors.Wrap(err, "source cluster node info")
		}

		if err := checkNodeMappings(sourceNodeInfo, targetNodeInfo, nodeMappings); err != nil {
			return errors.Wrap(err, "invalid node mappings")
		}

		return checkOne2OneRestoreCompatibility(sourceNodeInfo, targetNodeInfo)
	}, parallel.NopNotify)
}

func checkOne2OneRestoreCompatibility(source, target nodeValidationInfo) error {
	if source.ShardCount != target.ShardCount {
		return errors.Errorf("source ShardCount doesn't match target ShardCount")
	}
	if source.StorageSize > target.StorageSize {
		return errors.Errorf("source StorageSize greater than target StorageSize")
	}
	if !slices.Equal(source.Tokens, target.Tokens) {
		return errors.Errorf("source Tokens doesn't match target Tokens")
	}
	return nil
}

func checkNodeMappings(source, target nodeValidationInfo, nodeMappings []nodeMapping) error {
	targetIDx := slices.IndexFunc(nodeMappings, func(nodeMap nodeMapping) bool {
		return nodeMap.Source.DC == source.DC && nodeMap.Source.Rack == source.Rack && nodeMap.Source.HostID == source.HostID
	})
	if targetIDx == -1 {
		return errors.Errorf("mapping for source node is not found: %s %s %s", source.DC, source.Rack, source.HostID)
	}
	targetMap := nodeMappings[targetIDx].Target
	if targetMap.DC != target.DC || targetMap.Rack != target.Rack || targetMap.HostID != target.HostID {
		return errors.Errorf("mapping for target node is not found: %s %s %s", target.DC, target.Rack, target.HostID)
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

type nodeValidationInfo struct {
	DC          string
	Rack        string
	HostID      string
	ShardCount  int
	StorageSize uint64
	Tokens      []int64
}

func (w *worker) getTargetNodeInfo(ctx context.Context, host Host) (nodeValidationInfo, error) {
	ni, err := w.client.NodeInfo(ctx, host.Addr)
	if err != nil {
		return nodeValidationInfo{}, errors.Wrap(err, "get node info")
	}
	rack, err := w.client.HostRack(ctx, host.Addr)
	if err != nil {
		return nodeValidationInfo{}, errors.Wrap(err, "get node rack")
	}
	tokens, err := w.client.Tokens(ctx, host.Addr)
	if err != nil {
		return nodeValidationInfo{}, errors.Wrap(err, "get node tokens")
	}
	shardCount, err := w.client.ShardCount(ctx, host.Addr)
	if err != nil {
		return nodeValidationInfo{}, errors.Wrap(err, "get shard count")
	}

	// Make sure tokens are sorted,
	// so we can compare them later.
	slices.Sort(tokens)

	return nodeValidationInfo{
		HostID:      host.ID,
		DC:          host.DC,
		Rack:        rack,
		ShardCount:  int(shardCount),
		StorageSize: ni.StorageSize,
		Tokens:      tokens,
	}, nil
}

func (w *worker) getSourceNodeInfo(ctx context.Context, host Host, manifest *backupspec.ManifestInfo) (nodeValidationInfo, error) {
	mc, err := w.getManifestContent(ctx, host.Addr, manifest)
	if err != nil {
		return nodeValidationInfo{}, err
	}
	// Make sure tokens are sorted,
	// so we can compare them later.
	slices.Sort(mc.Tokens)

	return nodeValidationInfo{
		DC:          mc.DC,
		Rack:        mc.Rack,
		HostID:      mc.NodeID,
		ShardCount:  mc.ShardCount,
		StorageSize: mc.StorageSize,
		Tokens:      mc.Tokens,
	}, nil
}
