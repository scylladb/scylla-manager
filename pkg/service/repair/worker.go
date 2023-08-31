// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
)

type worker struct {
	in     <-chan job
	out    chan<- jobResult
	client *scyllaclient.Client
	// Marks tables for which handleRunningStatus didn't have any effect.
	// We want to limit the usage of handleRunningStatus to once per table
	// in order to avoid long waiting time on failed ranges.
	stopTrying map[string]struct{}
	progress   ProgressManager
	logger     log.Logger
}

// Run starts worker which awaits repair jobs and performs them.
// All encountered errors (except for ctx errors) are reported to generator.
func (w *worker) Run(ctx context.Context) {
	w.logger.Info(ctx, "Start")
	defer w.logger.Info(ctx, "Done")

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case j, ok := <-w.in:
			if !ok {
				return
			}

			w.progress.OnJobStart(ctx, j)
			r := jobResult{
				job: j,
				err: w.runRepair(ctx, j),
			}

			if ctx.Err() != nil {
				return
			}
			w.progress.OnJobEnd(ctx, r)
			w.out <- r
		}
	}
}

var errTableDeleted = errors.New("table deleted during repair")

func (w *worker) runRepair(ctx context.Context, j job) (out error) {
	if j.deleted {
		return nil
	}

	var (
		jobID int32
		err   error
	)
	// Decorate returned error
	defer func() {
		w.logger.Info(ctx, "Repair done")
		// Try to justify error by checking table deletion
		if out != nil && w.isTableDeleted(ctx, j) {
			out = errTableDeleted
		}
		out = errors.Wrapf(out, "master %s keyspace %s table %s command %d", j.master, j.keyspace, j.table, jobID)
	}()

	jobID, err = w.client.Repair(ctx, j.keyspace, j.table, j.master, j.replicaSet, j.tryOptimizeRanges())
	if err != nil {
		return errors.Wrap(err, "schedule repair")
	}

	w.logger.Info(ctx, "Repairing",
		"keyspace", j.keyspace,
		"table", j.table,
		"master", j.master,
		"hosts", j.replicaSet,
		"ranges", j.tryOptimizeRanges(),
		"job_id", jobID,
	)

	status, err := w.client.RepairStatus(ctx, j.master, jobID)
	if err != nil {
		return errors.Wrap(err, "get repair status")
	}

	switch status {
	case scyllaclient.CommandRunning:
		return w.handleRunningStatus(ctx, j)
	case scyllaclient.CommandFailed:
		return errors.Errorf("status %s", status)
	case scyllaclient.CommandSuccessful:
		return nil
	default:
		w.logger.Info(ctx, "Unexpected GET /storage_service/repair_status response", "response", status)
		return nil
	}
}

var errStatusRunning = errors.New("unexpected RUNNING status when synchronously waiting for repair end")

// handleRunningStatus is a workaround for a strange Scylla behaviour.
// Running status is sometimes returned from client.RepairStatus even
// when it should wait for repair to finish. It should be considered
// as an error in general, but this can also happen when waiting on
// repair status of recently deleted table. So before treating it as
// an error, we should wait a short while to check if this behaviour
// was indeed caused by table deletion.
func (w *worker) handleRunningStatus(ctx context.Context, j job) error {
	// Don't retry it on the same table, if it failed before
	if _, ok := w.stopTrying[j.keyspace+"."+j.table]; ok {
		return errStatusRunning
	}

	const (
		minWait      = 50 * time.Millisecond
		maxWait      = time.Second
		maxTotalTime = 30 * time.Second
		multiplier   = 2
		jitter       = 0.2
	)
	backoff := retry.NewExponentialBackoff(minWait, maxTotalTime, maxWait, multiplier, jitter)

	// Table deletion is visible only after a short while
	op := func() error {
		exists, err := w.client.TableExists(ctx, j.master, j.keyspace, j.table)
		if err != nil {
			return retry.Permanent(err)
		}
		if exists {
			return errors.New("table exists")
		}
		return nil
	}

	err := retry.WithNotify(ctx, op, backoff, func(error, time.Duration) {})
	if err != nil {
		w.stopTrying[j.keyspace+"."+j.table] = struct{}{}
		return errStatusRunning
	}
	return errTableDeleted
}

func (w *worker) isTableDeleted(ctx context.Context, j job) bool {
	exists, err := w.client.TableExists(ctx, j.master, j.keyspace, j.table)
	if err != nil {
		w.logger.Error(ctx, "Couldn't check for table deletion",
			"keyspace", j.keyspace,
			"table", j.table,
			"error", err,
		)
		return false
	}
	return !exists
}
