// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/dht"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/parallel"
)

var errTableDeleted = errors.New("table deleted during repair")

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
		err = w.runLegacyRepair(ctx, job.Ranges, job.Host, job.ShardsPercent)
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

	id, err := w.client.Repair(ctx, host, cfg)
	if err != nil {
		if w.tableDeleted(ctx, err, ttr.Keyspace, ttr.Table) {
			return errTableDeleted
		}

		return errors.Wrapf(err, "host %s: schedule repair", host)
	}

	logger := w.logger.With(
		"keyspace", ttr.Keyspace,
		"table", ttr.Table,
		"hosts", ttr.Replicas,
		"ranges", len(ttrs),
		"master", host,
		"job_id", id,
	)

	logger.Info(ctx, "Repairing")
	if err := w.waitRepairStatus(ctx, id, host, ttr.Keyspace, ttr.Table); err != nil {
		return errors.Wrapf(err, "host %s: keyspace %s table %s command %d", host, ttr.Keyspace, ttr.Table, id)
	}
	logger.Debug(ctx, "Repair done")
	return nil
}

func (w *worker) runLegacyRepair(ctx context.Context, ranges []*tableTokenRange, host string, shardsPercent float64) error {
	p := w.hostPartitioner[host]

	// Calculate max parallel shard repairs
	limit := int(p.ShardCount())
	if shardsPercent != 0 {
		l := float64(limit) * shardsPercent
		if l < 1 {
			l = 1
		}
		limit = int(l)

		w.logger.Debug(ctx, "Limiting parallel shard repairs", "total", p.ShardCount(), "limit", limit)
	}

	// Split ranges to shards
	shardRanges := splitToShards(ranges, p)
	if err := validateShards(ranges, shardRanges, p); err != nil {
		return err
	}

	return parallel.Run(len(shardRanges), limit, func(i int) error {
		if ctx.Err() != nil {
			return nil
		}

		v := shardRanges[i]
		if len(v) == 0 {
			return nil
		}

		return w.runRepair(log.WithFields(ctx, "subranges_of_shard", i), v, host)
	})
}

func (w *worker) waitRepairStatus(ctx context.Context, id int32, host, keyspace, table string) error {
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
				if w.tableDeleted(ctx, err, keyspace, table) {
					return errTableDeleted
				}
				return err
			}
			switch s {
			case scyllaclient.CommandRunning:
				// Continue
			case scyllaclient.CommandSuccessful:
				return nil
			case scyllaclient.CommandFailed:
				if w.tableDeleted(ctx, nil, keyspace, table) {
					return errTableDeleted
				}
				return errors.New("repair failed on Scylla - consult Scylla logs for details")
			default:
				return errors.Errorf("unknown command status %q", s)
			}
		}
	}
}

func (w *worker) tableDeleted(ctx context.Context, err error, keyspace, table string) bool {
	if err != nil {
		status, msg := scyllaclient.StatusCodeAndMessageOf(err)
		switch {
		case status >= 400 && scyllaclient.TableNotExistsRegex.MatchString(msg):
			return true
		case status < 400:
			return false
		}
	}

	exists, err := w.client.TableExists(ctx, keyspace, table)
	if err != nil {
		w.logger.Debug(ctx, "Failed to check if table exists after a Scylla repair failure", "error", err)
		return false
	}
	deleted := !exists

	if deleted {
		w.logger.Info(ctx, "Detected table deletion", "keyspace", keyspace, "table", table)
	}

	return deleted
}
