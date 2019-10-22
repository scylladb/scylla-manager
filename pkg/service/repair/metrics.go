// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/tickrun"
)

var (
	repairSegmentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_total",
		Help:      "Total number of segments to repair.",
	}, []string{"cluster", "task", "keyspace", "host", "shard"})

	repairSegmentsSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_success",
		Help:      "Number of repaired segments.",
	}, []string{"cluster", "task", "keyspace", "host", "shard"})

	repairSegmentsError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_error",
		Help:      "Number of segments that failed to repair.",
	}, []string{"cluster", "task", "keyspace", "host", "shard"})

	repairDurationSeconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "duration_seconds",
		Help:      "Duration of a single repair command.",
		MaxAge:    30 * time.Minute,
	}, []string{"cluster", "task", "keyspace", "host", "shard"})

	repairProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "progress",
		Help:      "Current repair progress.",
	}, []string{"cluster", "task", "keyspace", "host", "shard"})
)

func init() {
	prometheus.MustRegister(
		repairSegmentsTotal,
		repairSegmentsSuccess,
		repairSegmentsError,
		repairDurationSeconds,
		repairProgress,
	)
}

type progressFetcher func(ctx context.Context, run *Run) ([]*RunProgress, error)

func newProgressMetricsUpdater(ctx context.Context, run *Run, prog progressFetcher, logger log.Logger, interval time.Duration) (stop func()) {
	return tickrun.NewTicker(interval, updateFunc(ctx, run, prog, logger))
}

func updateFunc(ctx context.Context, run *Run, prog progressFetcher, logger log.Logger) func() {
	return func() {
		prog, err := prog(ctx, run)
		if err != nil {
			logger.Error(ctx, "Failed to get hosts progress", "error", err)
			return
		}

		taskID := run.TaskID.String()

		// Not aggregated keyspace host shard progress
		for _, p := range prog {
			repairProgress.With(prometheus.Labels{
				"cluster":  run.clusterName,
				"task":     run.TaskID.String(),
				"keyspace": run.Units[p.Unit].Keyspace,
				"host":     p.Host,
				"shard":    fmt.Sprint(p.Shard),
			}).Set(float64(p.PercentComplete()))
		}

		p := aggregateProgress(run, prog)

		// Aggregated keyspace host progress
		for i := range p.Units {
			unit := &p.Units[i]
			for _, n := range unit.Nodes {
				repairProgress.With(prometheus.Labels{
					"cluster":  run.clusterName,
					"task":     taskID,
					"keyspace": unit.Unit.Keyspace,
					"host":     n.Host,
					"shard":    "",
				}).Set(n.PercentComplete)
			}
		}

		// Aggregated keyspace progress
		for i := range p.Units {
			unit := &p.Units[i]
			repairProgress.With(prometheus.Labels{
				"cluster":  run.clusterName,
				"task":     taskID,
				"keyspace": unit.Unit.Keyspace,
				"host":     "",
				"shard":    "",
			}).Set(unit.PercentComplete)
		}

		// Aggregated total progress
		repairProgress.With(prometheus.Labels{
			"cluster":  run.clusterName,
			"task":     taskID,
			"keyspace": "",
			"host":     "",
			"shard":    "",
		}).Set(p.PercentComplete)
	}
}
