// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/internal/dht"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/scyllaclient"
	"go.uber.org/multierr"
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

// worker manages shardWorkers.
type worker struct {
	Config   *Config
	Run      *Run
	Unit     int
	Host     string
	Segments segments

	Service *Service
	Client  *scyllaclient.Client
	Logger  log.Logger

	shards []*shardWorker
}

func (w *worker) exec(ctx context.Context) error {
	if err := w.init(ctx); err != nil {
		return err
	}

	w.Logger.Info(ctx, "Repairing")

	// repair shards
	type shardError struct {
		shard int
		err   error
	}
	var (
		wch     = make(chan shardError)
		werr    error
		stopped bool
	)
	for i, s := range w.shards {
		i := i
		s := s
		go func() {
			wch <- shardError{i, s.exec(ctx)}
		}()
	}
	for range w.shards {
		r := <-wch
		if r.err != nil {
			if errors.Cause(r.err) == errStopped {
				stopped = true
			} else {
				werr = multierr.Append(werr, errors.Errorf("shard %d failed", r.shard))
			}
		}
	}

	if stopped || ctx.Err() != nil {
		w.Logger.Info(ctx, "Repair stopped")
		return errStopped
	}

	if werr == nil {
		w.Logger.Info(ctx, "Repair done")
	} else {
		w.Logger.Info(ctx, "Repair failed")
	}
	return werr
}

func (w *worker) init(ctx context.Context) error {
	// continue from a savepoint
	prog, err := w.Service.getHostProgress(ctx, w.Run, w.Unit, w.Host)
	if err != nil {
		return errors.Wrap(err, "failed to get host progress")
	}

	// split segments to shards
	p, err := w.partitioner(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get partitioner")
	}
	shards := w.splitSegmentsToShards(ctx, p)

	// check if savepoint can be used
	if err := validateShardProgress(shards, prog); err != nil {
		if len(prog) > 1 {
			w.Logger.Info(ctx, "Starting from scratch: invalid progress info", "error", err.Error(), "progress", prog)
		}
		prog = nil
	}

	w.shards = make([]*shardWorker, len(shards))

	for i, segments := range shards {
		// prepare progress
		var p *RunProgress
		if prog != nil {
			p = prog[i]
		} else {
			p = &RunProgress{
				ClusterID:    w.Run.ClusterID,
				TaskID:       w.Run.TaskID,
				RunID:        w.Run.ID,
				Unit:         w.Unit,
				Host:         w.Host,
				Shard:        i,
				SegmentCount: len(segments),
			}
		}

		labels := prometheus.Labels{
			"cluster":  w.Run.clusterName,
			"task":     w.Run.TaskID.String(),
			"keyspace": w.Run.Units[w.Unit].Keyspace,
			"host":     w.Host,
			"shard":    fmt.Sprint(i),
		}

		w.shards[i] = &shardWorker{
			parent:   w,
			segments: segments,
			progress: p,
			logger:   w.Logger.With("shard", i),

			repairSegmentsTotal:   repairSegmentsTotal.With(labels),
			repairSegmentsSuccess: repairSegmentsSuccess.With(labels),
			repairSegmentsError:   repairSegmentsError.With(labels),
			repairDurationSeconds: repairDurationSeconds.With(labels),
		}
	}

	return nil
}

func (w *worker) partitioner(ctx context.Context) (*dht.Murmur3Partitioner, error) {
	shardCount, err := w.Client.ShardCount(ctx, w.Host)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get shard count")
	}
	return dht.NewMurmur3Partitioner(shardCount, uint(w.Config.ShardingIgnoreMsbBits)), nil
}

func (w *worker) splitSegmentsToShards(ctx context.Context, p *dht.Murmur3Partitioner) []segments {
	shards := w.Segments.splitToShards(p)
	if err := w.Segments.validateShards(shards, p); err != nil {
		w.Logger.Info(ctx, "Suboptimal sharding", "error", err.Error())
	}

	for i := range shards {
		shards[i] = shards[i].merge()
		shards[i] = shards[i].split(int64(w.Config.SegmentSizeLimit))
	}

	return shards
}

// shardWorker repairs a single shard.
type shardWorker struct {
	parent   *worker
	segments segments
	progress *RunProgress
	logger   log.Logger

	repairSegmentsTotal   prometheus.Gauge
	repairSegmentsSuccess prometheus.Gauge
	repairSegmentsError   prometheus.Gauge
	repairDurationSeconds prometheus.Summary
}

func (w *shardWorker) exec(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			w.logger.Info(ctx, "Repair failed",
				"percent_complete", w.progress.PercentComplete(),
				"error", err,
			)
		}
	}()

	if w.progress.complete() {
		w.logger.Info(ctx, "Already repaired, skipping")
		w.updateMetrics()
		return nil
	}

	if w.progress.SegmentError > w.parent.Config.SegmentErrorLimit {
		w.logger.Info(ctx, "Starting from scratch: too many errors")
		w.resetProgress(ctx)
	}

	if w.progress.completeWithErrors() {
		return w.repair(ctx, w.newRetryIterator())
	}

	return w.repair(ctx, w.newForwardIterator())
}

func (w *shardWorker) newRetryIterator() *retryIterator {
	return &retryIterator{
		segments:          w.segments,
		progress:          w.progress,
		segmentsPerRepair: w.parent.Config.SegmentsPerRepair,
	}
}

func (w *shardWorker) newForwardIterator() *forwardIterator {
	return &forwardIterator{
		segments:          w.segments,
		progress:          w.progress,
		segmentsPerRepair: w.parent.Config.SegmentsPerRepair,
	}
}

func (w *shardWorker) repair(ctx context.Context, ri repairIterator) error {
	w.logger.Info(ctx, "Repairing", "percent_complete", w.progress.PercentComplete())
	w.updateMetrics()

	var (
		start int
		end   int
		id    int32
		err   error
		ok    bool
	)

	if w.progress.LastCommandID != 0 {
		id = w.progress.LastCommandID
	}

	next := func() {
		start, end, ok = ri.Next()
	}

	savepoint := func() {
		if ok {
			w.progress.LastStartToken = w.segments[start].StartToken
		} else {
			w.progress.LastStartToken = 0
		}

		if id != 0 {
			w.progress.LastCommandID = id
			w.progress.LastStartTime = timeutc.Now()
		} else {
			w.progress.LastCommandID = 0
			w.progress.LastStartTime = time.Time{}
		}

		w.updateProgress(ctx)
		w.updateMetrics()
	}

	next()

	for {
		// no more segments
		if !ok {
			break
		}

		if w.isStopped(ctx) {
			w.logger.Info(ctx, "Repair stopped", "percent_complete", w.progress.PercentComplete())
			return errStopped
		}

		if id == 0 {
			id, err = w.runRepair(ctx, start, end)
			if err != nil {
				ri.OnError()
				next()
				savepoint()
				return errors.Wrap(err, "repair request failed")
			}
		}

		savepoint()

		err, id = w.waitCommand(ctx, id), 0
		if err != nil {
			ri.OnError()

			if w.parent.Config.StopOnError {
				next()
				savepoint()
				return errors.New("repair stopped on error")
			}

			time.Sleep(w.parent.Config.ErrorBackoff)
		} else {
			ri.OnSuccess()
		}

		next()
		savepoint()

		if w.progress.SegmentError > w.parent.Config.SegmentErrorLimit {
			return errors.New("number of errors exceeded")
		}
	}

	if w.progress.SegmentError > 0 {
		return errors.New("repair finished with errors")
	}

	w.logger.Info(ctx, "Repair done")

	return nil
}

func (w *shardWorker) resetProgress(ctx context.Context) {
	w.progress.SegmentSuccess = 0
	w.progress.SegmentError = 0
	w.progress.SegmentErrorStartTokens = nil
	w.progress.LastStartToken = 0
	w.progress.LastStartTime = time.Time{}
	w.progress.LastCommandID = 0
}

func (w *shardWorker) isStopped(ctx context.Context) bool {
	stopped, err := w.parent.Service.isStopped(ctx, w.parent.Run)
	if ctx.Err() != nil {
		stopped, err = true, nil
	}
	if err != nil {
		w.logger.Error(ctx, "Service error", "error", err)
	}
	return stopped
}

func (w *shardWorker) runRepair(ctx context.Context, start, end int) (int32, error) {
	u := w.parent.Run.Units[w.parent.Unit]
	cfg := &scyllaclient.RepairConfig{
		Keyspace: u.Keyspace,
		Ranges:   w.segments[start:end].dump(),
	}
	if !u.allTables {
		cfg.Tables = u.Tables
	}
	return w.parent.Client.Repair(ctx, w.parent.Host, cfg)
}

func (w *shardWorker) waitCommand(ctx context.Context, id int32) error {
	start := timeutc.Now()
	defer func() {
		w.repairDurationSeconds.Observe(timeutc.Since(start).Seconds())
	}()

	t := time.NewTicker(w.parent.Config.PollInterval)
	defer t.Stop()

	u := w.parent.Run.Units[w.parent.Unit]

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s, err := w.parent.Client.RepairStatus(ctx, w.parent.Host, u.Keyspace, id)
			if err != nil {
				return err
			}
			switch s {
			case scyllaclient.CommandRunning:
				// continue
			case scyllaclient.CommandSuccessful:
				return nil
			case scyllaclient.CommandFailed:
				return errors.New("repair failed")
			default:
				return errors.Errorf("unknown status %q", s)
			}
		}
	}
}

func (w *shardWorker) updateProgress(ctx context.Context) {
	if err := w.parent.Service.putRunProgress(ctx, w.progress); err != nil {
		w.logger.Error(ctx, "Cannot update the run progress", "error", err)
	}
}

func (w *shardWorker) updateMetrics() {
	w.repairSegmentsTotal.Set(float64(w.progress.SegmentCount))
	w.repairSegmentsSuccess.Set(float64(w.progress.SegmentSuccess))
	w.repairSegmentsError.Set(float64(w.progress.SegmentError))
}
