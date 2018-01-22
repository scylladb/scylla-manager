// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/dht"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/scyllaclient"
)

// worker manages shardWorkers.
type worker struct {
	Unit     *Unit
	Run      *Run
	Config   *Config
	Service  *Service
	Cluster  *scyllaclient.Client
	Host     string
	Segments []*Segment

	logger log.Logger
	shards []*shardWorker
}

func (w *worker) exec(ctx context.Context) error {
	if err := w.init(ctx); err != nil {
		return err
	}

	// repair shards
	var (
		wg sync.WaitGroup
		ok = true
	)
	for _, s := range w.shards {
		if s.progress.complete() {
			s.logger.Info(ctx, "Already done, skipping")
			continue
		}

		s := s // range variable reuse
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if v := recover(); v != nil {
					s.logger.Error(ctx, "Panic", "panic", v)
					ok = false
				}
			}()
			if err := s.exec(ctx); err != nil {
				s.logger.Error(ctx, "Exec failed", "error", err)
				ok = false
			}
		}()
	}
	wg.Wait()

	if !ok {
		return errors.New("shard error, see log for details")
	}

	return nil
}

func (w *worker) init(ctx context.Context) error {
	// continue from a savepoint
	prog, err := w.Service.GetProgress(ctx, w.Unit, w.Run.ID, w.Host)
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
			w.logger.Info(ctx, "Starting from scratch: invalid progress info", "error", err.Error())
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
				UnitID:       w.Run.UnitID,
				RunID:        w.Run.ID,
				Host:         w.Host,
				Shard:        i,
				SegmentCount: len(segments),
			}
		}

		w.shards[i] = &shardWorker{
			parent:   w,
			segments: segments,
			progress: p,
			logger:   w.logger.With("shard", i),
		}
	}

	return nil
}

func (w *worker) partitioner(ctx context.Context) (*dht.Murmur3Partitioner, error) {
	c, err := w.Cluster.HostConfig(ctx, w.Host)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get host config")
	}

	// create partitioner
	var (
		shardCount            uint
		shardingIgnoreMsbBits uint
		ok                    bool
	)
	if shardCount, ok = c.ShardCount(); !ok {
		return nil, errors.New("config missing shard_count")
	}
	if shardingIgnoreMsbBits, ok = c.Murmur3PartitionerIgnoreMsbBits(); !ok {
		return nil, errors.New("config missing murmur3_partitioner_ignore_msb_bits")
	}

	return dht.NewMurmur3Partitioner(shardCount, shardingIgnoreMsbBits), nil
}

func (w *worker) splitSegmentsToShards(ctx context.Context, p *dht.Murmur3Partitioner) [][]*Segment {
	shards := splitSegmentsToShards(w.Segments, p)
	if err := validateShards(w.Segments, shards, p); err != nil {
		w.logger.Info(ctx, "Suboptimal sharding", "error", err.Error())
	}

	for i := range shards {
		shards[i] = mergeSegments(shards[i])
		shards[i] = splitSegments(shards[i], *w.Config.SegmentSizeLimit)
	}

	return shards
}

const maxFailedSegments = 100

// shardWorker repairs a single shard.
type shardWorker struct {
	parent   *worker
	segments []*Segment
	progress *RunProgress
	logger   log.Logger
}

func (w *shardWorker) exec(ctx context.Context) error {
	if w.progress.SegmentError > maxFailedSegments {
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
		segments: w.segments,
		progress: w.progress,
	}
}

func (w *shardWorker) newForwardIterator() *forwardIterator {
	return &forwardIterator{
		segments: w.segments,
		progress: w.progress,
	}
}

func (w *shardWorker) repair(ctx context.Context, ri repairIterator) error {
	var (
		start int
		end   int
		id    int32
		err   error
		ok    bool
	)

	savepoint := func() {
		w.progress.LastStartTime = time.Now()
		w.progress.LastStartToken = w.segments[start].StartToken
		w.progress.LastCommandID = id
		w.updateProgress(ctx)
	}

	for {
		start, end, ok = ri.Next()
		if !ok {
			break
		}

		if w.isStopped(ctx) {
			w.logger.Info(ctx, "Stopped")
			return nil
		}

		if p := w.progress.PercentComplete(); p%10 == 0 {
			w.logger.Info(ctx, "Progress", "percent", w.progress.PercentComplete())
		}

		if w.progress.LastCommandID != 0 {
			id = w.progress.LastCommandID
		} else {
			id, err = w.runRepair(ctx, start, end)
			if err != nil {
				if ctx.Err() != nil {
					w.logger.Info(ctx, "Aborted")
					return nil
				}

				ri.OnError()
				w.updateProgress(ctx)

				return errors.Wrap(err, "repair request failed")
			}
		}

		savepoint()

		err = w.waitCommand(ctx, id)
		if ctx.Err() != nil {
			w.logger.Info(ctx, "Aborted")
			return nil
		}
		if err != nil {
			w.logger.Info(ctx, "Repair failed", "error", err)
			ri.OnError()
		} else {
			ri.OnSuccess()
		}
		w.updateProgress(ctx)

		if w.progress.SegmentError > maxFailedSegments {
			return errors.New("number of errors exceeded")
		}
	}

	if w.progress.SegmentError > 0 {
		return errors.New("repair finished with errors")
	}

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
	if ctx.Err() != nil {
		return true
	}

	stopped, err := w.parent.Service.isStopped(ctx, w.parent.Unit, w.parent.Run.ID)
	if err != nil {
		w.logger.Error(ctx, "Service error", "error", err)
	}
	return stopped
}

func (w *shardWorker) runRepair(ctx context.Context, start, end int) (int32, error) {
	return w.parent.Cluster.Repair(ctx, w.parent.Host, &scyllaclient.RepairConfig{
		Keyspace: w.parent.Run.Keyspace,
		Tables:   w.parent.Run.Tables,
		Ranges:   dumpSegments(w.segments[start:end]),
	})
}

const pollInterval = time.Second

func (w *shardWorker) waitCommand(ctx context.Context, id int32) error {
	t := time.NewTicker(pollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s, err := w.parent.Cluster.RepairStatus(ctx, w.parent.Host, w.parent.Run.Keyspace, id)
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
