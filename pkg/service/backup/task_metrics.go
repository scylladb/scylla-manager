// Copyright (C) 2026 ScyllaDB

package backup

import (
	"sync"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// tableHostKey identifies the smallest unit of reported backup progress.
type tableHostKey struct {
	host     string
	keyspace string
	table    string
}

// fileBytes describes the byte counters of a backup progress update.
type fileBytes struct {
	size     int64
	uploaded int64
	skipped  int64
}

func (b fileBytes) sub(o fileBytes) fileBytes {
	return fileBytes{
		size:     b.size - o.size,
		uploaded: b.uploaded - o.uploaded,
		skipped:  b.skipped - o.skipped,
	}
}

func (b *fileBytes) add(o fileBytes) {
	b.size += o.size
	b.uploaded += o.uploaded
	b.skipped += o.skipped
}

// progress returns backup completion in percents (0-100).
func (b fileBytes) progress() float64 {
	if b.size <= 0 {
		return 0
	}
	p := float64(b.uploaded+b.skipped) / float64(b.size) * 100
	// Watch out for rounding errors and for files that grew during upload.
	if p > 100 {
		return 100
	}
	return p
}

// taskMetricsAggregator turns per table, per node backup progress updates
// into task scoped metrics.
//
// Backup progress is reported per keyspace, table and host, with absolute
// values, and the same table is reported many times as its upload advances.
// The aggregator keeps the last reported value of every table so that it can
// apply progress as a delta, which keeps the running totals O(1) per update.
//
// The memory it holds is of the same order as the per table backup metrics
// that SM already keeps in its Prometheus collectors.
type taskMetricsAggregator struct {
	clusterID  uuid.UUID
	taskID     uuid.UUID
	backupType string
	metrics    metrics.BackupMetrics

	mu    sync.Mutex
	last  map[tableHostKey]fileBytes
	host  map[string]fileBytes
	total fileBytes
}

func newTaskMetricsAggregator(clusterID, taskID uuid.UUID, method Method, m metrics.BackupMetrics) *taskMetricsAggregator {
	a := &taskMetricsAggregator{
		clusterID:  clusterID,
		taskID:     taskID,
		backupType: backupTypeFromMethod(method),
		metrics:    m,
		last:       make(map[tableHostKey]fileBytes),
		host:       make(map[string]fileBytes),
	}
	// The byte metrics of the previous run are still there, and they add up
	// to 100% complete. Clear them before the run starts reporting.
	a.metrics.ResetTaskMetrics(a.clusterID, a.taskID, a.backupType)
	return a
}

// update applies an absolute progress report of a single table on a single node.
func (a *taskMetricsAggregator) update(host, keyspace, table string, b fileBytes) {
	k := tableHostKey{host: host, keyspace: keyspace, table: table}

	a.mu.Lock()
	delta := b.sub(a.last[k])
	a.last[k] = b

	hb := a.host[host]
	hb.add(delta)
	a.host[host] = hb

	a.total.add(delta)
	total := a.total
	a.mu.Unlock()

	a.metrics.SetTaskFilesProgress(a.clusterID, a.taskID, a.backupType, host, hb.size, hb.uploaded, hb.skipped)
	a.metrics.SetTaskProgress(a.clusterID, a.taskID, a.backupType, total.progress())
}

func backupTypeFromMethod(m Method) string {
	switch m {
	case MethodNative:
		return metrics.BackupTypeNative
	case MethodRclone:
		return metrics.BackupTypeRclone
	default:
		return metrics.BackupTypeAuto
	}
}
