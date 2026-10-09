// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
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
	clusterID uuid.UUID
	taskID    uuid.UUID
	metrics   metrics.BackupMetrics

	mu    sync.Mutex
	last  map[tableHostKey]fileBytes
	total fileBytes
}

func newTaskMetricsAggregator(clusterID, taskID uuid.UUID, m metrics.BackupMetrics) *taskMetricsAggregator {
	return &taskMetricsAggregator{
		clusterID: clusterID,
		taskID:    taskID,
		metrics:   m,
		last:      make(map[tableHostKey]fileBytes),
	}
}

// update applies an absolute progress report of a single table on a single node.
//
// The metric is set while the mutex is held. Setting it afterwards would let
// a slower update overwrite the result of a faster one that started later,
// rolling the reported progress backwards.
func (a *taskMetricsAggregator) update(host, keyspace, table string, b fileBytes) {
	k := tableHostKey{host: host, keyspace: keyspace, table: table}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.total.add(b.sub(a.last[k]))
	a.last[k] = b
	a.metrics.SetTaskProgress(a.clusterID, a.taskID, a.total.progress())
}

// setTaskPropertiesMetric reports the properties the task will run with.
// It is the backup service that owns them, so the defaults are the real ones
// rather than a copy kept somewhere else.
func (s *Service) setTaskPropertiesMetric(ctx context.Context, clusterID, taskID uuid.UUID, properties json.RawMessage) {
	p := defaultTaskProperties()
	if err := json.Unmarshal(properties, &p); err != nil {
		// The run itself will fail on the same properties and say why. The
		// metric is informational, so it just stays as it was.
		s.logger.Info(ctx, "Cannot report task properties", "task", taskID, "error", err)
		return
	}
	p.expandDefaultTaskProperties()
	s.metrics.SetTaskProperties(clusterID, taskID, taskPropertiesMetric(p))
}

// taskPropertiesMetric describes the properties for the metric. It takes them
// after expandDefaultTaskProperties, so what it reports is what the run will
// use.
func taskPropertiesMetric(p taskProperties) metrics.BackupTaskProperties {
	locations := make([]string, len(p.Location))
	for i, l := range p.Location {
		locations[i] = l.String()
	}
	r := p.extractRetention()

	return metrics.BackupTaskProperties{
		// An empty filter means every keyspace or datacenter.
		Keyspace:          joinOrAll(p.Keyspace),
		DC:                joinOrAll(p.DC),
		Location:          strings.Join(locations, ","),
		Retention:         strconv.Itoa(r.Retention),
		RetentionDays:     strconv.Itoa(r.RetentionDays),
		Method:            string(p.Method),
		PurgeOnly:         strconv.FormatBool(p.PurgeOnly),
		SkipSchema:        strconv.FormatBool(p.SkipSchema),
		RetentionLockMode: string(p.RetentionLockMode),
	}
}

func joinOrAll(v []string) string {
	if len(v) == 0 {
		return "all"
	}
	return strings.Join(v, ",")
}
