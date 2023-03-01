// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

var errTableDeleted = errors.New("table deleted during repair")

type worker struct {
	run                       *Run
	in                        <-chan job
	out                       chan<- jobResult
	client                    *scyllaclient.Client
	progress                  progressManager
	hostPartitioner           map[string]*dht.Murmur3Partitioner
	hostFeatures              map[string]scyllaclient.ScyllaFeatures
	repairType                Type
	pollInterval              time.Duration
	longPollingTimeoutSeconds int
	logger                    log.Logger
}

func newWorker(run *Run, in <-chan job, out chan<- jobResult, client *scyllaclient.Client,
	manager progressManager, hostPartitioner map[string]*dht.Murmur3Partitioner,
	hostFeatures map[string]scyllaclient.ScyllaFeatures, repairType Type,
	pollInterval time.Duration, longPollingTimeoutSeconds int, logger log.Logger,
) *worker {
	return &worker{
		run:                       run,
		in:                        in,
		out:                       out,
		client:                    client,
		progress:                  manager,
		hostPartitioner:           hostPartitioner,
		hostFeatures:              hostFeatures,
		repairType:                repairType,
		pollInterval:              pollInterval,
		longPollingTimeoutSeconds: longPollingTimeoutSeconds,
		logger:                    logger,
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
			w.progress.OnJobResult(ctx, r)
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
	if w.repairType == TypeRowLevel && job.Allowance.ShardsPercent == 0 {
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

	w.progress.OnScyllaJobStart(ctx, job, jobID)
	defer w.progress.OnScyllaJobEnd(ctx, job, jobID)

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
	logger.Info(ctx, "Repair done")

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

	// In a very rare case when there is a strange partitioner
	// do not split to shards.
	if p == nil {
		err := w.runRepair(ctx, job)
		if err != nil {
			w.logger.Error(ctx, "Run legacy repair, no sharding due to unsupported partitioner", "error", err)
		}
		return err
	}

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
	var (
		waitSeconds int
		ticker      *time.Ticker
	)

	if w.hostFeatures[host].RepairLongPolling {
		waitSeconds = w.longPollingTimeoutSeconds
	} else {
		ticker = time.NewTicker(w.pollInterval)
		defer ticker.Stop()
	}

	for {
		if waitSeconds > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		s, err := w.client.RepairStatus(ctx, host, keyspace, id, waitSeconds)
		if err != nil {
			if w.tableDeleted(ctx, err, keyspace, table) {
				return errTableDeleted
			}
			return err
		}
		running, err := w.repairStatus(ctx, s, id, host, keyspace, table)
		if err != nil || !running {
			return err
		}
	}
}

func (w *worker) repairStatus(ctx context.Context, s scyllaclient.CommandStatus, id int32, host, keyspace, table string) (bool, error) {
	switch s {
	case scyllaclient.CommandRunning:
		return true, nil
	case scyllaclient.CommandSuccessful:
		return false, nil
	case scyllaclient.CommandFailed:
		if w.tableDeleted(ctx, nil, keyspace, table) {
			return false, errTableDeleted
		}
		return false, errors.Errorf("Scylla error - check logs on host %s for job %d", host, id)
	default:
		return false, errors.Errorf("unknown command status %q - check logs on host %s for job %d", s, host, id)
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
