// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"golang.org/x/sync/errgroup"
)

// validateClusters checks if 1-1-restore can be performed on provided clusters.
func (w *worker) validateClusters(ctx context.Context, locations []LocationInfo, nodeMappings []nodeMapping) error {
	if err := w.client.VerifyNodesAvailability(ctx); err != nil {
		return errors.Wrap(err, "all nodes must be available")
	}

	sourceNodeInfo, targetNodeInfo, err := w.collectNodeInfo(ctx, locations)
	if err != nil {
		return errors.Wrap(err, "collect nodes info")
	}

	sourceNodeInfo, err = applyNodeMapping(sourceNodeInfo, getSourceMappings(nodeMappings))
	if err != nil {
		return errors.Wrap(err, "apply node mappings")
	}

	slices.SortFunc(sourceNodeInfo, compareNodesInfo)
	slices.SortFunc(targetNodeInfo, compareNodesInfo)

	if err := clustersAreEqual(sourceNodeInfo, targetNodeInfo); err != nil {
		return errors.Wrap(err, "clusters not equal")
	}

	return nil
}

func applyNodeMapping(sourceNodeInfo []nodeInfo, sourceMappings map[node]node) ([]nodeInfo, error) {
	var result []nodeInfo
	if len(sourceNodeInfo) != len(sourceMappings) {
		return nil, errors.Errorf("nodes count (%d) != node count in mappings (%d)", len(sourceNodeInfo), len(sourceMappings))
	}
	for _, source := range sourceNodeInfo {
		target, ok := sourceMappings[node{DC: source.DC, Rack: source.Rack, HostID: source.HostID}]
		if !ok {
			return nil, errors.Errorf("mapping for source node (%v) is not found", source)
		}
		source.DC, source.Rack, source.HostID = target.DC, target.Rack, target.HostID
		result = append(result, source)
	}
	return result, nil
}

func compareNodesInfo(a, b nodeInfo) int {
	return cmp.Or(
		cmp.Compare(a.DC, b.DC),
		cmp.Compare(a.Rack, b.Rack),
		cmp.Compare(a.HostID, b.HostID),
		cmp.Compare(a.CPUCount, b.CPUCount),
		cmp.Compare(a.StorageSize, b.StorageSize),
		slices.Compare(a.Tokens, b.Tokens),
	)
}

func clustersAreEqual(sourceNodeInfo, targetNodeInfo []nodeInfo) error {
	if len(sourceNodeInfo) != len(targetNodeInfo) {
		return errors.Errorf("clusters have different nodes count: source %d != target %d", len(sourceNodeInfo), len(targetNodeInfo))
	}
	for i, source := range sourceNodeInfo {
		target := targetNodeInfo[i]
		if source.DC != target.DC {
			return errors.Errorf("source DC doesn't match target DC")
		}
		if source.Rack != target.Rack {
			return errors.Errorf("source Rack doesn't match target Rack")
		}
		if source.HostID != target.HostID {
			return errors.Errorf("source HostID doesn't match target HostID")
		}
		if source.CPUCount != target.CPUCount {
			return errors.Errorf("source CPUCount doesn't match target CPUCount")
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

func (w *worker) collectNodeInfo(ctx context.Context, locations []LocationInfo) (sourceCluster, targetCluster []nodeInfo, err error) {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		sourceInfo, err := w.getSourceClusterNodeInfo(ctx, locations)
		if err != nil {
			return errors.Wrap(err, "source cluster node info")
		}

		sourceCluster = sourceInfo
		return nil
	})
	g.Go(func() error {
		targetInfo, err := w.getTargetClusterNodeInfo(ctx, locations)
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

type nodeInfo struct {
	DC          string
	Rack        string
	HostID      string
	CPUCount    int64
	StorageSize uint64
	Tokens      []int64
}

func (w *worker) getTargetClusterNodeInfo(ctx context.Context, locations []LocationInfo) ([]nodeInfo, error) {
	hosts := uniqHosts(locations)
	result := make([]nodeInfo, len(hosts))
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

		result[i] = nodeInfo{
			HostID:      h.ID,
			DC:          h.DC,
			Rack:        rack,
			CPUCount:    ni.CPUCount,
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

func uniqHosts(locations []LocationInfo) []Host {
	var result []Host
	seen := map[Host]struct{}{}
	for _, l := range locations {
		for _, h := range l.Hosts {
			_, ok := seen[h]
			if ok {
				continue
			}
			seen[h] = struct{}{}
			result = append(result, h)
		}
	}
	return result
}

func (w *worker) getSourceClusterNodeInfo(ctx context.Context, locations []LocationInfo) ([]nodeInfo, error) {
	hostsManifests := assignHostToManifest(locations)
	result := make([]nodeInfo, len(hostsManifests))
	err := parallel.Run(len(hostsManifests), parallel.NoLimit, func(i int) error {
		h, m := hostsManifests[i].Host, hostsManifests[i].Manifest
		mc := &backupspec.ManifestContentWithIndex{}
		r, err := w.client.RcloneOpen(ctx, h.Addr, m.Location.RemotePath(m.Path()))
		if err != nil {
			return errors.Wrap(err, "open manifest")
		}
		defer r.Close()
		if err := mc.Read(r); err != nil {
			return errors.Wrap(err, "read manifest")
		}
		result[i] = nodeInfo{
			DC:          mc.DC,
			Rack:        mc.Rack,
			HostID:      mc.NodeID,
			CPUCount:    int64(mc.CPUCount),
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

type hostManifest struct {
	Host     Host
	Manifest *backupspec.ManifestInfo
}

func assignHostToManifest(locations []LocationInfo) []hostManifest {
	var result []hostManifest
	usedHosts := map[Host]struct{}{}
	for _, l := range locations {
		for _, m := range l.Manifest {
			result = append(result, hostManifest{
				Host:     unusedHostOrRandom(l.Hosts, usedHosts),
				Manifest: m,
			})
		}
	}
	return result
}

func unusedHostOrRandom(hosts []Host, usedHosts map[Host]struct{}) Host {
	for _, n := range hosts {
		_, ok := usedHosts[n]
		if ok {
			continue
		}
		usedHosts[n] = struct{}{}
		return n
	}
	return hosts[rand.IntN(len(hosts))]
}
