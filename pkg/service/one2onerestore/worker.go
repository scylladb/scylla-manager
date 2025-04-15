// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
	"go.uber.org/multierr"
)

type worker struct {
	managerSession gocqlx.Session

	client         *scyllaclient.Client
	clusterSession gocqlx.Session

	logger log.Logger

	runInfo struct {
		ClusterID, TaskID, RunID uuid.UUID
	}
}

func (w *worker) parseTarget(ctx context.Context, properties json.RawMessage) (Target, error) {
	target := defaultTarget()
	if err := json.Unmarshal(properties, &target); err != nil {
		return Target{}, errors.Wrap(err, "unmarshal json")
	}
	keyspaces, err := w.client.Keyspaces(ctx)
	if err != nil {
		return Target{}, errors.Wrap(err, "get keyspaces")
	}
	if err := target.validateProperties(keyspaces); err != nil {
		return Target{}, errors.Wrap(err, "invalid target")
	}
	skip, err := skipRestorePatterns(ctx, w.client, w.clusterSession)
	if err != nil {
		return Target{}, errors.Wrap(err, "skip restore patterns")
	}
	w.logger.Info(ctx, "Extended excluded tables pattern", "pattern", skip)
	target.Keyspace = append(target.Keyspace, skip...)
	return target, nil
}

// restore is an actual 1-1-restore stages.
func (w *worker) restore(ctx context.Context, workload []hostWorkload, target Target) (err error) {
	if err := w.setTombstoneGCModeRepair(ctx, workload); err != nil {
		return errors.Wrap(err, "tombstone_gc mode")
	}

	views, err := w.dropViews(ctx, workload)
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
func (w *worker) prepareHostWorkload(ctx context.Context, manifests []*backupspec.ManifestInfo, hosts []Host, target Target) ([]hostWorkload, error) {
	targetBySourceHostID, err := mapTargetHostToSource(hosts, target.NodesMapping)
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
		h.ShardCount = mc.ShardCount
		hw := hostWorkload{
			host:            h,
			manifestInfo:    m,
			manifestContent: mc,
		}

		if err := mc.ForEachIndexIter(target.Keyspace, func(fm backupspec.FilesMeta) {
			hw.tablesToRestore = append(hw.tablesToRestore, scyllaTableWithSize{scyllaTable: scyllaTable{keyspace: fm.Keyspace, table: fm.Table}, size: fm.Size})
		}); err != nil {
			return errors.Wrap(err, "read manifest content")
		}

		result[i] = hw

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

func skipRestorePatterns(ctx context.Context, client *scyllaclient.Client, session gocqlx.Session) ([]string, error) {
	keyspaces, err := client.KeyspacesByType(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get keyspaces by type")
	}
	tables, err := client.AllTables(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get all tables")
	}

	var skip []string
	// Skip local data.
	// Note that this also covers the raft based tables (e.g. system and system_schema).
	for _, ks := range keyspaces[scyllaclient.KeyspaceTypeAll] {
		if !slices.Contains(keyspaces[scyllaclient.KeyspaceTypeNonLocal], ks) {
			skip = append(skip, ks)
		}
	}

	// Skip outdated tables.
	// Note that even though system_auth is not used in Scylla 6.0,
	// it might still be present there (leftover after upgrade).
	// That's why SM should always skip known outdated tables so that backups
	// from older Scylla versions don't cause unexpected problems.
	if err := isRestoreAuthAndServiceLevelsFromSStablesSupported(ctx, client); err != nil {
		if errors.Is(err, errRestoreAuthAndServiceLevelsUnsupportedScyllaVersion) {
			skip = append(skip, "system_auth", "system_distributed.service_levels")
		} else {
			return nil, errors.Wrap(err, "check auth and service levels restore support")
		}
	}

	// Skip system cdc tables
	systemCDCTableRegex := regexp.MustCompile(`(^|_)cdc(_|$)`)
	for ks, tabs := range tables {
		// Local keyspaces were already excluded
		if !slices.Contains(keyspaces[scyllaclient.KeyspaceTypeNonLocal], ks) {
			continue
		}
		// Here we only skip system cdc tables
		if slices.Contains(keyspaces[scyllaclient.KeyspaceTypeUser], ks) {
			continue
		}
		for _, t := range tabs {
			if systemCDCTableRegex.MatchString(t) {
				skip = append(skip, ks+"."+t)
			}
		}
	}

	// Skip user cdc tables
	skip = append(skip, "*.*_scylla_cdc_log")

	// Skip views
	views, err := query.GetAllViews(session)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster views")
	}
	skip = append(skip, views.List()...)

	// Exclude collected patterns
	out := make([]string, 0, len(skip))
	for _, p := range skip {
		out = append(out, "!"+p)
	}
	return out, nil
}

// errRestoreAuthAndServiceLevelsUnsupportedScyllaVersion means that restore auth and service levels procedure is not safe for used Scylla configuration.
var errRestoreAuthAndServiceLevelsUnsupportedScyllaVersion = errors.Errorf("restoring authentication and service levels is not supported for given ScyllaDB version")

// isRestoreAuthAndServiceLevelsFromSStablesSupported checks if restore auth and service levels procedure is supported for used Scylla configuration.
// Because of #3869 and #3875, there is no way fo SM to safely restore auth and service levels into cluster with
// version higher or equal to OSS 6.0 or ENT 2024.2.
func isRestoreAuthAndServiceLevelsFromSStablesSupported(ctx context.Context, client *scyllaclient.Client) error {
	const (
		ossConstraint = ">= 6.0, < 2000"
		entConstraint = ">= 2024.2, > 1000"
	)

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}
	for _, n := range status {
		ni, err := client.NodeInfo(ctx, n.Addr)
		if err != nil {
			return errors.Wrapf(err, "get node %s info", n.Addr)
		}

		ossNotSupported, err := version.CheckConstraint(ni.ScyllaVersion, ossConstraint)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}
		entNotSupported, err := version.CheckConstraint(ni.ScyllaVersion, entConstraint)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}

		if ossNotSupported || entNotSupported {
			return errRestoreAuthAndServiceLevelsUnsupportedScyllaVersion
		}
	}

	return nil
}
