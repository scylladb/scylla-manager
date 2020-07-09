// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
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
	in           <-chan job
	out          chan<- jobResult
	client       *scyllaclient.Client
	logger       log.Logger
	progress     progressManager
	pollInterval time.Duration
}

func newWorker(in <-chan job, out chan<- jobResult, client *scyllaclient.Client, logger log.Logger, manager progressManager, pollInterval time.Duration) worker {
	return worker{
		in:           in,
		out:          out,
		client:       client,
		logger:       logger,
		progress:     manager,
		pollInterval: pollInterval,
	}
}

func (w worker) Run(ctx context.Context) error {
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

func (w worker) runJob(ctx context.Context, job job) error {
	ttr := job.Ranges[0]

	if err := w.progress.OnStartJob(ctx, job); err != nil {
		return errors.Wrapf(err, "host %s: starting progress", job.Host)
	}

	cfg := scyllaclient.RepairConfig{
		Keyspace: ttr.Keyspace,
		Tables:   []string{ttr.Table},
		Hosts:    ttr.Replicas,
		Ranges:   dumpRanges(job.Ranges),
	}

	id, err := w.client.Repair(ctx, job.Host, cfg)
	if err != nil {
		return errors.Wrapf(err, "host %s: issue repair", job.Host)
	}

	w.logger.Debug(ctx, "Repair",
		"keyspace", ttr.Keyspace,
		"table", ttr.Table,
		"hosts", ttr.Replicas,
		"ranges", len(job.Ranges),
		"job_id", id,
	)

	// TODO change to long polling
	t := time.NewTicker(w.pollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s, err := w.client.RepairStatus(ctx, job.Host, ttr.Keyspace, id)
			if err != nil {
				return err
			}
			w.logger.Debug(ctx, "Repair status", "status", s)
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
