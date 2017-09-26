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
	var wg sync.WaitGroup
	for _, s := range w.shards {
		wg.Add(1)
		go s.exec(ctx, &wg)
	}
	wg.Wait()

	w.logger.Info(ctx, "Done")

	return nil
}

func (w *worker) init(ctx context.Context) error {
	p, err := w.partitioner(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get partitioner")
	}

	shards := splitSegmentsToShards(w.Segments, p)
	if err := validateShards(w.Segments, shards, p); err != nil {
		w.logger.Info(ctx, "Suboptimal sharding", "error", err)
	}

	w.shards = make([]*shardWorker, len(shards))

	for i, segments := range shards {
		segments = mergeSegments(segments)
		segments = splitSegments(segments, *w.Config.SegmentSizeLimit)

		w.shards[i] = &shardWorker{
			parent:   w,
			segments: segments,
			progress: &RunProgress{
				ClusterID:    w.Run.ClusterID,
				UnitID:       w.Run.UnitID,
				RunID:        w.Run.ID,
				Host:         w.Host,
				Shard:        i,
				SegmentCount: len(segments),
			},
			logger: w.logger.With("shard", i),
		}

		if err := w.Service.putRunProgress(ctx, w.shards[i].progress); err != nil {
			return errors.Wrapf(err, "failed to initialise segments progress %s", w.shards[i].progress)
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

// shardWorker repairs a single shard
type shardWorker struct {
	parent   *worker
	segments []*Segment
	progress *RunProgress
	logger   log.Logger
}

func (w *shardWorker) exec(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	w.logger.Info(ctx, "Starting repair")
	w.logger.Debug(ctx, "Segment stats", "stats", segmentsStats(w.segments))

	var (
		start = 0
		end   = segmentsPerRequest
	)
	for start < len(w.segments) {
		// issue a repair
		id, err := w.parent.Cluster.Repair(ctx, w.parent.Host, &scylla.RepairConfig{
			Keyspace: w.parent.Run.Keyspace,
			Tables:   w.parent.Run.Tables,
			Ranges:   dumpSegments(w.segments[start:end]),
		})
		if err != nil {
			// TODO limited tolerance to errors, 30m errors non stop ignore...
			w.logger.Info(ctx, "Repair request failed", "error", err)
			w.progress.SegmentError += end - start
		} else {
			// sevepoint
			w.progress.LastCommandID = id
			w.progress.LastStartTime = time.Now()
			w.progress.LastStartToken = w.segments[start].StartToken
			w.updateProgress(ctx)

			if err := w.waitCommand(ctx, id); err != nil {
				w.logger.Info(ctx, "Repair failed", "error", err)
				w.progress.SegmentError += end - start
			} else {
				w.progress.SegmentSuccess += end - start
			}
		}

		w.updateProgress(ctx)

		start = end
		end += segmentsPerRequest
		if end > len(w.segments) {
			end = len(w.segments)
		}

		w.logger.Info(ctx, "Progress", "percent", w.percentDone())
	}

	w.logger.Info(ctx, "Done")
}

func (w *shardWorker) waitCommand(ctx context.Context, id int32) error {
	t := time.NewTicker(checkIntervalSeconds * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.New("aborted")
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

func (w *shardWorker) percentDone() int {
	if w.progress.SegmentCount == 0 {
		return 100
	}

	return 100 * (w.progress.SegmentSuccess + w.progress.SegmentError) / w.progress.SegmentCount
}
