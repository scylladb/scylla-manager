// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// tableKey identifies a single table repaired by the tablet repair task.
type tableKey struct {
	keyspace string
	table    string
}

// tableSizes keeps cluster wide on disk size of every table repaired
// in a single tablet repair run.
// Sizes are calculated once, at the beginning of the run, and they are not
// refreshed later on, so that the progress reported during the run is
// weighted against a constant total.
// The tablet repair task does not support pause/resume - a stopped task is
// started from scratch with a new run ID - so there is no need to persist
// the sizes across runs.
type tableSizes struct {
	size  map[tableKey]int64
	total int64
}

// newTableSizes calculates on disk size of all tables from the target.
// Only live hosts are queried, as a down host does not respond to the
// table disk size API at all.
func newTableSizes(ctx context.Context, client *scyllaclient.Client, target Target) (tableSizes, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return tableSizes{}, errors.Wrap(err, "get status")
	}
	hosts := status.Live().Hosts()

	var hkts scyllaclient.HostKeyspaceTables
	for ks, tabs := range target.KsTabs {
		for _, tab := range tabs {
			for _, h := range hosts {
				hkts = append(hkts, scyllaclient.HostKeyspaceTable{Host: h, Keyspace: ks, Table: tab})
			}
		}
	}

	report, err := client.TableDiskSizeReport(ctx, hkts)
	if err != nil {
		return tableSizes{}, errors.Wrap(err, "calculate tables size")
	}

	out := tableSizes{size: make(map[tableKey]int64)}
	for _, sr := range report {
		out.size[tableKey{keyspace: sr.Keyspace, table: sr.Table}] += sr.Size
		out.total += sr.Size
	}
	return out, nil
}

// tableSize returns cluster wide on disk size of a single table.
func (ts tableSizes) tableSize(ks, tab string) int64 {
	return ts.size[tableKey{keyspace: ks, table: tab}]
}

// totalSize returns the sum of the sizes of all repaired tables.
func (ts tableSizes) totalSize() int64 {
	return ts.total
}

// weight returns the fraction (0-1) of the whole task that a single table
// accounts for. When the total size is not known (e.g. all repaired tables
// are empty), all tables are weighted equally.
func (ts tableSizes) weight(ks, tab string) float64 {
	if len(ts.size) == 0 {
		return 0
	}
	if ts.total == 0 {
		return 1 / float64(len(ts.size))
	}
	return float64(ts.tableSize(ks, tab)) / float64(ts.total)
}

// tabletRepairMode is the value of the "mode" label of the repair task
// progress metric reported by the tablet repair task.
// The task always relies on the Scylla side default incremental mode (#4683).
var tabletRepairMode = metrics.RepairMode("")
