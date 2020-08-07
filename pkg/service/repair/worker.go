// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
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
}

func newWorker(run *Run, in <-chan job, out chan<- jobResult, client *scyllaclient.Client,
	logger log.Logger, manager progressManager, pollInterval time.Duration,
	hostPartitioner map[string]*dht.Murmur3Partitioner) worker {
	return worker{
		run:          run,
		in:           in,
		out:          out,
		client:       client,
		logger:       logger,
		progress:     manager,
		pollInterval: pollInterval,

		hostPartitioner: hostPartitioner,
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

	var (
		err error
		msg string
	)
	if w.hostPartitioner[job.Host] == nil {
		err = w.runRepair(ctx, job.Ranges, job.Host)
		msg = "Run row-level repair"
	} else {
		err = w.runLegacyRepair(ctx, job.Ranges, job.Host)
		msg = "Run legacy repair"
	}
	if err != nil {
		w.logger.Error(ctx, msg, "error", err)
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
