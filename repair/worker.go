// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/dht"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/scylla"
)

const (
	segmentsPerRequest   = 50
	checkIntervalSeconds = 1
)

// worker manages shardWorkers.
type worker struct {
	Unit     *Unit
	Run      *Run
	Config   *Config
	Service  *Service
	Cluster  *scylla.Client
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
		if s.progress.Done() {
			s.logger.Info(ctx, "Already done, skipping")
			continue
		}

		// range variable reuse
		s := s
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if v := recover(); v != nil {
					s.logger.Error(ctx, "Panic", "panic", v)
					ok = false
				}
			}()
			s.exec(ctx)
		}()
	}
	wg.Wait()

	if !ok {
		return errors.New("shard panic")
	}

	return nil
}

func (w *worker) init(ctx context.Context) error {
	// split segments to shards
	p, err := w.partitioner(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get partitioner")
	}
	shards := w.splitSegmentsToShards(ctx, p)

	// continue from a savepoint
	prog, err := w.Service.GetProgress(ctx, w.Unit, w.Run.ID, w.Host)
	if err != nil {
		return errors.Wrap(err, "failed to get host progress")
	}
	if err := validateShardProgress(shards, prog); err != nil {
		if len(prog) > 1 {
			w.logger.Info(ctx, "Starting from scratch: invalid progress info", "error", err.Error())
		}
		prog = nil
	}

	w.shards = make([]*shardWorker, len(shards))

	for i, segments := range shards {
		// prepare progress
		p := &RunProgress{
			ClusterID:    w.Run.ClusterID,
			UnitID:       w.Run.UnitID,
			RunID:        w.Run.ID,
			Host:         w.Host,
			Shard:        i,
			SegmentCount: len(segments),
		}
		if prog != nil {
			p = prog[i]
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

// shardWorker repairs a single shard
type shardWorker struct {
	parent   *worker
	segments []*Segment
	progress *RunProgress
	logger   log.Logger
}

func (w *shardWorker) exec(ctx context.Context) {
	var (
		start = w.startSegment(ctx)
		end   = start + segmentsPerRequest
		id    int32
		err   error
	)

	w.logger.Info(ctx, "Starting repair", "start_segment", start)
	w.logger.Debug(ctx, "Segment stats", "stats", segmentsStats(w.segments))

	next := func() {
		start, end = end, end+segmentsPerRequest
	}

	savepoint := func() {
		w.progress.LastStartTime = time.Now()
		w.progress.LastStartToken = w.segments[start].StartToken
		w.progress.LastCommandID = id
		w.updateProgress(ctx)
	}

	for start < len(w.segments) {
		w.logger.Info(ctx, "Progress", "percent", w.progress.PercentDone())

		if w.isStopped(ctx) {
			w.logger.Info(ctx, "Stopped")
			break
		}

		if end > len(w.segments) {
			end = len(w.segments)
		}

		if w.progress.LastCommandID != 0 {
			id = w.progress.LastCommandID
		} else {
			id, err = w.runRepair(ctx, start, end)
			if err != nil {
				if ctx.Err() != nil {
					w.logger.Info(ctx, "Aborted")
					break
				}

				w.logger.Info(ctx, "Repair request failed", "error", err)

				w.progress.SegmentError += end - start
				w.updateProgress(ctx)

				next()
				continue
			}
		}

		savepoint()

		err = w.waitCommand(ctx, id)
		if ctx.Err() != nil {
			w.logger.Info(ctx, "Aborted")
			break
		}
		if err != nil {
			w.logger.Info(ctx, "Repair failed", "error", err)
			w.progress.SegmentError += end - start
		} else {
			w.progress.SegmentSuccess += end - start
		}
		w.progress.LastCommandID = 0
		w.updateProgress(ctx)

		next()
	}
}

func (w *shardWorker) startSegment(ctx context.Context) int {
	if !w.progress.started() {
		return 0
	}

	for i := 0; i < len(w.segments); i++ {
		if w.segments[i].StartToken == w.progress.LastStartToken {
			return i
		}
	}

	// this shall never happen as it's checked by validateShardProgress
	w.resetProgress(ctx)

	return 0
}

func (w *shardWorker) resetProgress(ctx context.Context) {
	w.logger.Error(ctx, "Starting from scratch: progress reset...")
	w.progress.SegmentSuccess = 0
	w.progress.SegmentError = 0
	w.progress.LastStartToken = 0
	w.progress.LastStartTime = time.Time{}
	w.progress.LastCommandID = 0
}

func (w *shardWorker) isStopped(ctx context.Context) bool {
	stopped, err := w.parent.Service.isStopped(ctx, w.parent.Unit, w.parent.Run.ID)
	if err != nil {
		w.logger.Error(ctx, "Service error", "error", err)
	}
	return stopped
}

func (w *shardWorker) runRepair(ctx context.Context, start, end int) (int32, error) {
	return w.parent.Cluster.Repair(ctx, w.parent.Host, &scylla.RepairConfig{
		Keyspace: w.parent.Run.Keyspace,
		Tables:   w.parent.Run.Tables,
		Ranges:   dumpSegments(w.segments[start:end]),
	})
}

func (w *shardWorker) waitCommand(ctx context.Context, id int32) error {
	t := time.NewTicker(checkIntervalSeconds * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// TODO limited tolerance to errors, 30m errors non stop ignore...
			s, err := w.parent.Cluster.RepairStatus(ctx, w.parent.Host, w.parent.Run.Keyspace, id)
			if err != nil {
				return err
			}
			switch s {
			case scylla.CommandRunning:
				// continue
			case scylla.CommandSuccessful:
				return nil
			case scylla.CommandFailed:
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
