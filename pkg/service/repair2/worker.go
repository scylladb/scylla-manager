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
	in     <-chan job
	out    chan<- jobResult
	client *scyllaclient.Client
	logger log.Logger
}

func newWorker(in <-chan job, out chan<- jobResult, client *scyllaclient.Client, logger log.Logger) worker {
	return worker{
		in:     in,
		out:    out,
		client: client,
		logger: logger,
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
	tr := job.Ranges[0]

	cfg := scyllaclient.RepairConfig{
		Keyspace: tr.Keyspace,
		Tables:   []string{tr.Table},
		Hosts:    tr.Replicas,
		Ranges:   dumpRanges(job.Ranges),
	}

	id, err := w.client.Repair(ctx, job.Host, cfg)
	if err != nil {
		return errors.Wrapf(err, "host %s: issue repair", job.Host)
	}

	w.logger.Debug(ctx, "Repair",
		"keyspace", tr.Keyspace,
		"table", tr.Table,
		"hosts", tr.Replicas,
		"ranges", len(job.Ranges),
		"job_id", id,
	)

	// TODO change to long polling
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s, err := w.client.RepairStatus(ctx, job.Host, tr.Keyspace, id)
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
