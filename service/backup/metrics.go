// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/tickrun"
)

var (
	backupPercentProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "percent_progress",
		Help:      "Current backup percent progress.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupBytesDone = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "bytes_done",
		Help:      "Number of bytes uploaded so far.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupBytesLeft = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "bytes_left",
		Help:      "Number of bytes left for backup completion.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupAvgUploadBandwidth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "avg_upload_bandwidth",
		Help:      "Average speed in bytes/sec since start of the upload",
	}, []string{"cluster", "task", "host", "keyspace"})
)

func init() {
	prometheus.MustRegister(
		backupPercentProgress,
		backupBytesDone,
		backupBytesLeft,
		backupAvgUploadBandwidth,
	)
}

func newBackupMetricUpdater(ctx context.Context, run *Run, vis ProgressVisitor, logger log.Logger, interval time.Duration) (stop func()) {
	return tickrun.NewTicker(interval, updateFunc(ctx, run, vis, logger))
}

func saveMetrics(p progress, labels prometheus.Labels) {
	totalDone, totalLeft := p.ByteProgress()
	backupBytesLeft.With(labels).Set(float64(totalLeft))
	backupBytesDone.With(labels).Set(float64(totalDone))
	backupPercentProgress.With(labels).Set(float64(p.PercentComplete()))
	backupAvgUploadBandwidth.With(labels).Set(p.AvgUploadBandwidth())
}

func updateFunc(ctx context.Context, run *Run, vis ProgressVisitor, logger log.Logger) func() {
	return func() {
		p, err := aggregateProgress(run, vis)
		if err != nil {
			logger.Error(ctx, "Failed to aggregate backup progress metrics", "error", err)
			return
		}

		clusterID := run.ClusterID.String()
		taskID := run.TaskID.String()

		// Aggregated total progress
		totalLabels := prometheus.Labels{
			"cluster":  clusterID,
			"task":     taskID,
			"host":     "",
			"keyspace": "",
		}
		saveMetrics(p.progress, totalLabels)

		for _, host := range p.Hosts {
			// Aggregated keyspace progress
			for _, keyspace := range host.Keyspaces {
				keyspaceLabels := prometheus.Labels{
					"cluster":  clusterID,
					"task":     taskID,
					"host":     host.Host,
					"keyspace": keyspace.Keyspace,
				}
				saveMetrics(keyspace.progress, keyspaceLabels)
			}

			// Aggregated host progress
			hostLabels := prometheus.Labels{
				"cluster":  clusterID,
				"task":     taskID,
				"host":     host.Host,
				"keyspace": "",
			}
			saveMetrics(host.progress, hostLabels)
		}
	}
}
