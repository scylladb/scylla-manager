// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/dht"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
)

var errTableDeleted = errors.New("table deleted during repair")

func maxParallelRepairs(ranges []scyllaclient.TokenRange) int {
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
	progress        progressManager
	pollInterval    time.Duration
	hostPartitioner map[string]*dht.Murmur3Partitioner
	logger          log.Logger
}

func newWorker(run *Run, in <-chan job, out chan<- jobResult, client *scyllaclient.Client,
	manager progressManager, hostPartitioner map[string]*dht.Murmur3Partitioner,
	pollInterval time.Duration, logger log.Logger) *worker {
	return &worker{
		run:             run,
		in:              in,
		out:             out,
		client:          client,
		progress:        manager,
		pollInterval:    pollInterval,
		hostPartitioner: hostPartitioner,
		logger:          logger,
	}
}

func (w *worker) Run(ctx context.Context) error {
	w.logger.Info(ctx, "Start")

	defer func() {
		w.logger.Info(ctx, "Done")
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-w.in:
			if !ok {
				return nil
			}

			r := jobResult{
				job: job,
				Err: w.handleJob(ctx, job),
			}

			if err := w.progress.OnJobResult(ctx, r); err != nil {
				return errors.Wrapf(err, "host %s: job result", job.Host)
			}

			select {
			case w.out <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (w *worker) handleJob(ctx context.Context, job job) error {
	var err error
	if w.hostPartitioner[job.Host] == nil {
		err = w.rowLevelRepair(ctx, job)
	} else {
		err = w.legacyRepair(ctx, job)
	}
	return err
}

func (w *worker) runRepair(ctx context.Context, job job) error {
	if len(job.Ranges) == 0 {
		return errors.Errorf("host %s: nothing to repair", job.Host)
	}

	var (
		host = job.Host
		ttrs = job.Ranges
		ttr  = job.Ranges[0]
	)

	cfg := scyllaclient.RepairConfig{
		Keyspace: ttr.Keyspace,
		Tables:   []string{ttr.Table},
		Hosts:    ttr.Replicas,
		Ranges:   dumpRanges(ttrs),
	}

	jobID, err := w.client.Repair(ctx, host, cfg)
	if err != nil {
		if w.tableDeleted(ctx, err, ttr.Keyspace, ttr.Table) {
			return errTableDeleted
		}

		return errors.Wrapf(err, "host %s: schedule repair", host)
	}

	if err := w.progress.OnScyllaJobStart(ctx, job, jobID); err != nil {
		w.logger.Error(ctx, "Failed to register OnScyllaJobStart event",
			"host", host, "job_id", jobID, "error", err,
		)
	}
	defer func() {
		if err := w.progress.OnScyllaJobEnd(ctx, job, jobID); err != nil {
			w.logger.Error(ctx, "Failed to register OnScyllaJobEnd event",
				"host", host, "job_id", jobID, "error", err,
			)
		}
	}()

	logger := w.logger.With(
		"keyspace", ttr.Keyspace,
		"table", ttr.Table,
		"hosts", ttr.Replicas,
		"ranges", len(ttrs),
		"master", host,
		"job_id", jobID,
	)

	logger.Info(ctx, "Repairing")
	if err := w.waitRepairStatus(ctx, jobID, host, ttr.Keyspace, ttr.Table); err != nil {
		return errors.Wrapf(err, "host %s: keyspace %s table %s command %d", host, ttr.Keyspace, ttr.Table, jobID)
	}
	logger.Debug(ctx, "Repair done")

	return nil
}

func (w *worker) rowLevelRepair(ctx context.Context, job job) error {
	err := w.runRepair(ctx, job)
	if err != nil {
		w.logger.Error(ctx, "Run row-level repair", "error", err)
	}
	return err
}

func (w *worker) legacyRepair(ctx context.Context, job job) error {
	p := w.hostPartitioner[job.Host]

	// Calculate max parallel shard repairs
	limit := int(p.ShardCount())
	if job.Allowance.ShardsPercent != 0 {
		l := float64(limit) * job.Allowance.ShardsPercent
		if l < 1 {
			l = 1
		}
		limit = int(l)

		w.logger.Debug(ctx, "Limiting parallel shard repairs", "total", p.ShardCount(), "limit", limit)
	}

	// Split ranges to shards
	shardRanges, err := splitToShardsAndValidate(job.Ranges, p)
	if err != nil {
		return errors.Wrap(err, "split to shards")
	}

	err = parallel.Run(len(shardRanges), limit, func(i int) error {
		if ctx.Err() != nil {
			return nil
		}

		if len(shardRanges[i]) == 0 {
			return nil
		}

		shardJob := job
		shardJob.Ranges = shardRanges[i]
		return w.runRepair(log.WithFields(ctx, "subranges_of_shard", i), shardJob)
	})
	if err != nil {
		w.logger.Error(ctx, "Run legacy repair", "error", err)
	}
	return err
}

func (w *worker) waitRepairStatus(ctx context.Context, id int32, host, keyspace, table string) error {
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
