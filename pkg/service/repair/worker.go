// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/i64set"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/dht"
	"github.com/scylladb/mermaid/pkg/scyllaclient"

	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"golang.org/x/sync/errgroup"
)

func workerCount(ranges []scyllaclient.TokenRange) int {
	var replicas = make(map[uint64][]string)
	for _, tr := range ranges {
		replicas[replicaHash(tr.Replicas)] = tr.Replicas
	}

	busy := strset.New()
	size := 0
	for _, items := range replicas {
		if !busy.HasAny(items...) {
			busy.Add(items...)
			size++
		}
	}

	return size
}

type worker struct {
	run             *Run
	in              <-chan job
	out             chan<- jobResult
	client          *scyllaclient.Client
	logger          log.Logger
	progress        progressManager
	pollInterval    time.Duration
	hostPartitioner map[string]*dht.Murmur3Partitioner
	failFast        bool
}

func newWorker(run *Run, in <-chan job, out chan<- jobResult, client *scyllaclient.Client,
	logger log.Logger, manager progressManager, pollInterval time.Duration,
	hostPartitioner map[string]*dht.Murmur3Partitioner, failFast bool) worker {
	return worker{
		run:          run,
		in:           in,
		out:          out,
		client:       client,
		logger:       logger,
		progress:     manager,
		pollInterval: pollInterval,

		hostPartitioner: hostPartitioner,
		failFast:        failFast,
	}
}

func (w *worker) Run(ctx context.Context) error {
	w.logger.Info(ctx, "Start")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-w.in:
			if !ok {
				w.logger.Info(ctx, "Done")
				return nil
			}

			w.out <- jobResult{
				job: job,
				Err: w.runJob(ctx, job),
			}
		}
	}
}

func (w *worker) runJob(ctx context.Context, job job) error {
	if err := w.progress.OnStartJob(ctx, job); err != nil {
		return errors.Wrapf(err, "host %s: starting progress", job.Host)
	}

	const (
		jobAttempts       = 2
		jobAttemptTimeout = 5 * time.Second
	)

	var err error
	for attempt := 1; attempt <= jobAttempts; attempt++ {
		msg := ""
		if w.hostPartitioner[job.Host] == nil {
			err = w.runRepair(ctx, job.Ranges, job.Host)
			msg = "Run row-level repair"
		} else {
			err = w.runLegacyRepair(ctx, job.Ranges, job.Host)
			msg = "Run legacy repair"
		}
		if err != nil {
			w.logger.Error(ctx, msg, "error", err, "attempt", attempt)
			if w.failFast {
				break
			}
			time.Sleep(jobAttemptTimeout)
			continue
		}
		break
	}

	return err
}

func (w *worker) runRepair(ctx context.Context, ttrs []*tableTokenRange, host string) error {
	if len(ttrs) == 0 {
		return fmt.Errorf("host %s: nothing to repair", host)
	}
	ttr := ttrs[0]

	cfg := scyllaclient.RepairConfig{
		Keyspace: ttr.Keyspace,
		Tables:   []string{ttr.Table},
		Hosts:    ttr.Replicas,
		Ranges:   dumpRanges(ttrs),
	}

	start := timeutc.Now()
	defer func() {
		repairDurationSeconds.With(prometheus.Labels{
			"cluster":  w.run.clusterName,
			"task":     w.run.TaskID.String(),
			"keyspace": ttr.Keyspace,
			"host":     host,
		}).Observe(timeutc.Since(start).Seconds())
	}()

	id, err := w.client.Repair(ctx, host, cfg)
	if err != nil {
		return errors.Wrapf(err, "host %s: repair issue", host)
	}
	w.logger.Debug(ctx, "Repair",
		"keyspace", ttr.Keyspace,
		"table", ttr.Table,
		"hosts", ttr.Replicas,
		"ranges", len(ttrs),
		"job_id", id,
	)
	if err := w.waitRepairStatus(ctx, id, host, ttr.Keyspace); err != nil {
		return errors.Wrapf(err, "host %s: checking repair status", host)
	}

	return nil
}

func (w *worker) runLegacyRepair(ctx context.Context, ranges []*tableTokenRange, host string) error {
	shardRanges := splitToShards(ranges, w.hostPartitioner[host])

	if err := validateShards(ranges, shardRanges, w.hostPartitioner[host]); err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, shard := range shardRanges {
		g.Go(func(shard []*tableTokenRange) func() error {
			return func() error {
				return w.runRepair(gCtx, shard, host)
			}
		}(shard))
	}

	return g.Wait()
}

func (w *worker) waitRepairStatus(ctx context.Context, id int32, host, keyspace string) error {
	// TODO change to long polling
	t := time.NewTicker(w.pollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s, err := w.client.RepairStatus(ctx, host, keyspace, id)
			if err != nil {
				return err
			}
			switch s {
			case scyllaclient.CommandRunning:
				// continue
			case scyllaclient.CommandSuccessful:
				return nil
			case scyllaclient.CommandFailed:
				return errors.New("repair failed consult Scylla logs")
			default:
				return errors.Errorf("unknown command status %q", s)
			}
		}
	}
}

func splitToShards(ttrs []*tableTokenRange, p *dht.Murmur3Partitioner) [][]*tableTokenRange {
	res := make([][]*tableTokenRange, p.ShardCount())

	for _, ttr := range ttrs {
		start := ttr.StartToken
		end := ttr.EndToken
		shard := p.ShardOf(end - 1)

		for start < end {
			prev := p.PrevShard(shard)
			token := p.TokenForPrevShard(end, shard)

			if token > start {
				res[shard] = append(res[shard], &tableTokenRange{
					Keyspace:   ttr.Keyspace,
					Table:      ttr.Table,
					Pos:        ttr.Pos,
					Replicas:   ttr.Replicas,
					StartToken: token,
					EndToken:   end,
				})
			} else {
				res[shard] = append(res[shard], &tableTokenRange{
					Keyspace:   ttr.Keyspace,
					Table:      ttr.Table,
					Pos:        ttr.Pos,
					Replicas:   ttr.Replicas,
					StartToken: start,
					EndToken:   end,
				})
			}

			end = token
			shard = prev
		}
	}

	return res
}

// validateShards checks that the shard split of segments is sound.
func validateShards(ttrs []*tableTokenRange, shards [][]*tableTokenRange, p *dht.Murmur3Partitioner) error {
	startTokens := i64set.New()
	endTokens := i64set.New()

	// Check that the s belong to the correct shards
	for shard, s := range shards {
		for _, r := range s {
			if p.ShardOf(r.StartToken) != uint(shard) {
				return errors.Errorf("wrong shard of a start token %d, expected %d, got %d", r.StartToken, p.ShardOf(r.StartToken), shard)
			}
			if p.ShardOf(r.EndToken-1) != uint(shard) {
				return errors.Errorf("wrong shard of an end token %d, expected %d, got %d", r.EndToken-1, p.ShardOf(r.EndToken-1), shard)
			}

			// Extract tokens
			startTokens.Add(r.StartToken)
			endTokens.Add(r.EndToken)
		}
	}

	// Check that shards contain the original start and end tokens
	for _, ttr := range ttrs {
		if !startTokens.Has(ttr.StartToken) {
			return errors.Errorf("no start token %d", ttr.StartToken)
		}
		if !endTokens.Has(ttr.EndToken) {
			return errors.Errorf("no end token %d", ttr.StartToken)
		}

		startTokens.Remove(ttr.StartToken)
		endTokens.Remove(ttr.EndToken)
	}

	// Check that the range is continuous
	var err error

	startTokens.Each(func(item int64) bool {
		if !endTokens.Has(item) {
			err = errors.Errorf("missing end token for start token %d", item)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	endTokens.Each(func(item int64) bool {
		if !startTokens.Has(item) {
			err = errors.Errorf("missing start token end token %d", item)
			return false
		}
		return true
	})

	return err
}
