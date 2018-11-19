// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
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
	}, []string{"cluster", "task", "keyspace", "host"})
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

type progressMetricsUpdater struct {
	run    *Run
	prog   func(ctx context.Context, run *Run) ([]*RunProgress, error)
	logger log.Logger
	stop   chan struct{}
	done   chan struct{}
}

func newProgressMetricsUpdater(run *Run, prog func(ctx context.Context, run *Run) ([]*RunProgress, error), logger log.Logger) *progressMetricsUpdater {
	return &progressMetricsUpdater{
		run:    run,
		prog:   prog,
		logger: logger,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (u *progressMetricsUpdater) Run(ctx context.Context, interval time.Duration) {
	defer close(u.done)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-u.stop:
			u.Update(ctx)
			return
		case <-t.C:
			u.Update(ctx)
		}
	}
}

func (u *progressMetricsUpdater) Update(ctx context.Context) {
	prog, err := u.prog(ctx, u.run)
	if err != nil {
		u.logger.Error(ctx, "Failed to get hosts progress", "error", err)
		return
	}

	p := aggregateProgress(u.run, prog)

	// aggregated keyspace host progress
	for _, unit := range p.Units {
		for _, n := range unit.Nodes {
			repairProgress.With(prometheus.Labels{
				"cluster":  u.run.clusterName,
				"task":     u.run.TaskID.String(),
				"keyspace": unit.Unit.Keyspace,
				"host":     n.Host,
			}).Set(float64(n.PercentComplete))
		}
	}

	// aggregated keyspace progress
	for _, unit := range p.Units {
		repairProgress.With(prometheus.Labels{
			"cluster":  u.run.clusterName,
			"task":     u.run.TaskID.String(),
			"keyspace": unit.Unit.Keyspace,
			"host":     "",
		}).Set(float64(unit.PercentComplete))
	}

	// aggregated total progress
	repairProgress.With(prometheus.Labels{
		"cluster":  u.run.clusterName,
		"task":     u.run.TaskID.String(),
		"keyspace": "",
		"host":     "",
	}).Set(float64(p.PercentComplete))
}

func (u *progressMetricsUpdater) Stop() {
	close(u.stop)
	<-u.done
}
