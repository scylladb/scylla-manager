// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
)

type worker struct {
	config Config
	target Target
	client *scyllaclient.Client
	// Marks tables for which handleRunningStatus didn't have any effect.
	// We want to limit the usage of handleRunningStatus to once per table
	// in order to avoid long waiting time on failed ranges.
	stopTrying map[string]struct{}
	progress   ProgressManager
	logger     log.Logger
}

func (w *worker) HandleJob(ctx context.Context, j job) jobResult {
	w.progress.OnJobStart(ctx, j)
	r := jobResult{
		job: j,
		err: w.runRepair(ctx, j),
	}
	w.progress.OnJobEnd(ctx, r)
	return r
}

func (w *worker) Done(ctx context.Context) {
	w.logger.Info(ctx, "Done")
}

var errTableDeleted = errors.New("table deleted during repair")

func (w *worker) runRepair(ctx context.Context, j job) (out error) {
	if j.jobType == skipJobType {
		return nil
	}

	var (
		jobID int32
		err   error
	)
	// Decorate returned error
	defer func() {
		w.logger.Info(ctx, "Repair done", "job_id", jobID)
		// Try to justify error by checking table deletion
		if out != nil && w.isTableDeleted(ctx, j) {
			out = errTableDeleted
		}
		out = errors.Wrapf(out, "master %s keyspace %s table %s command %d", j.master, j.keyspace, j.table, jobID)
	}()

	var ranges []scyllaclient.TokenRange
	switch {
	case j.jobType == tabletJobType:
		return w.fullTabletTableRepair(ctx, j.keyspace, j.table, j.master.String())
	case j.jobType == smallTableJobType:
		ranges = nil
	case j.jobType == mergeRangesJobType:
		ranges = []scyllaclient.TokenRange{
			{
				StartToken: dht.Murmur3MinToken,
				EndToken:   dht.Murmur3MaxToken,
			},
		}
	default:
		ranges = j.ranges
	}

	jobID, err = w.client.Repair(ctx, j.keyspace, j.table, j.master.String(), slices.MapToString(j.replicaSet), ranges, j.intensity, j.jobType == smallTableJobType)
	if err != nil {
		return errors.Wrap(err, "schedule repair")
	}

	w.logger.Info(ctx, "Repairing",
		"keyspace", j.keyspace,
		"table", j.table,
		"master", j.master,
		"hosts", j.replicaSet,
		"ranges", len(ranges),
		"intensity", j.intensity,
		"job_id", jobID,
	)

	status, err := w.client.RepairStatus(ctx, j.master.String(), jobID)
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
		exists, err := w.client.TableExists(ctx, j.master.String(), j.keyspace, j.table)
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
	exists, err := w.client.TableExists(ctx, j.master.String(), j.keyspace, j.table)
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

func (w *worker) fullTabletTableRepair(ctx context.Context, keyspace, table, host string) error {
	hostFilter, err := w.hostFilter(ctx)
	if err != nil {
		return errors.Wrap(err, "create host filter")
	}

	id, err := w.client.TabletRepair(ctx, keyspace, table, host, w.target.DC, hostFilter)
	if err != nil {
		return errors.Wrap(err, "schedule tablet repair task")
	}

	w.logger.Info(ctx, "Repairing entire tablet table",
		"keyspace", keyspace,
		"table", table,
		"task ID", id,
	)

	for {
		status, err := w.client.ScyllaWaitTask(ctx, host, id, int64(w.config.LongPollingTimeoutSeconds))
		if err != nil {
			w.scyllaAbortTask(host, id)
			return errors.Wrap(err, "get tablet repair task status")
		}

		switch scyllaclient.ScyllaTaskState(status.State) {
		case scyllaclient.ScyllaTaskStateDone:
			return nil
		case scyllaclient.ScyllaTaskStateFailed:
			return errors.Errorf("tablet repair task finished with status %q", scyllaclient.ScyllaTaskStateFailed)
		case scyllaclient.ScyllaTaskStateCreated, scyllaclient.ScyllaTaskStateRunning:
			continue
		default:
			w.scyllaAbortTask(host, id)
			return errors.Errorf("unexpected tablet repair task status %q", status.State)
		}
	}
}

func (w *worker) hostFilter(ctx context.Context) ([]string, error) {
	if len(w.target.IgnoreHosts) == 0 {
		return nil, nil
	}
	status, err := w.client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}
	return status.Up().HostIDs(), nil
}

func (w *worker) scyllaAbortTask(host, id string) {
	if err := w.client.ScyllaAbortTask(context.Background(), host, id); err != nil {
		w.logger.Error(context.Background(), "Failed to abort task",
			"host", host,
			"id", id,
			"error", err,
		)
	}
}
