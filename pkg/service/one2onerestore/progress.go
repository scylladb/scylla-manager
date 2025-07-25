// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"time"

	stderr "errors"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// initProgressAndMetrics adds entries to RunTableProgress and RunViewProgress, so that API call
// to show progress can return some information at this point.
// Sets initial values for download_remaining_bytes and view_build_status metrics.
func (w *worker) initProgressAndMetrics(ctx context.Context, workload []hostWorkload) error {
	views, err := w.getViews(ctx, workload)
	if err != nil {
		return errors.Wrap(err, "get views")
	}
	for _, v := range views {
		if err := w.insertRunViewProgress(ctx, &RunViewProgress{
			ClusterID: w.runInfo.ClusterID,
			TaskID:    w.runInfo.TaskID,
			RunID:     w.runInfo.RunID,

			Keyspace: v.Keyspace,
			Table:    v.BaseTable,
			View:     v.View,

			ViewType:        string(v.Type),
			ViewBuildStatus: scyllaclient.StatusUnknown,
		}); err != nil {
			w.logger.Error(ctx, "Failed to init view progress", "err", err)
		}
		w.metrics.SetViewBuildStatus(w.runInfo.ClusterID, v.Keyspace, v.View, metrics.BuildStatusUnknown)
	}

	for _, work := range workload {
		host := work.host.Addr
		for _, tableInfo := range work.tablesToRestore {
			if err := w.insertRunTableProgress(ctx, &RunTableProgress{
				ClusterID: w.runInfo.ClusterID,
				TaskID:    w.runInfo.TaskID,
				RunID:     w.runInfo.RunID,

				Keyspace: tableInfo.keyspace,
				Table:    tableInfo.table,
				Host:     host,

				TableSize: tableInfo.size,
			}); err != nil {
				w.logger.Error(ctx, "Failed to init table progress", "err", err)
			}
			w.metrics.SetDownloadRemainingBytes(metrics.One2OneRestoreBytesLabels{
				ClusterID:   w.runInfo.ClusterID.String(),
				SnapshotTag: work.manifestInfo.SnapshotTag,
				Location:    work.manifestInfo.Location.String(),
				DC:          work.manifestInfo.DC,
				Node:        host,
				Keyspace:    tableInfo.keyspace,
				Table:       tableInfo.table,
			}, float64(tableInfo.size))
		}
	}

	w.metrics.SetProgress(w.runInfo.ClusterID, workload[0].manifestInfo.SnapshotTag, 0)

	return nil
}

func (w *worker) reCreateViewProgress(ctx context.Context, view View) *RunViewProgress {
	started := timeutc.Now()
	pr := RunViewProgress{
		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		StartedAt: &started,

		Keyspace: view.Keyspace,
		Table:    view.BaseTable,
		View:     view.View,
		ViewType: string(view.Type),

		ViewBuildStatus: view.BuildStatus,
	}

	if err := w.insertRunViewProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Recreate view progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateReCreateViewProgress(ctx context.Context, pr *RunViewProgress, status scyllaclient.ViewBuildStatus) {
	completedAt := timeutc.Now()
	if status == scyllaclient.StatusSuccess {
		pr.CompletedAt = &completedAt
	}
	pr.ViewBuildStatus = status
	if err := w.insertRunViewProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update recreate view progress", "err", err, "pr", pr)
	}
}

func (w *worker) restoreTableProgress(ctx context.Context, host string, table backupspec.FilesMeta) *RunTableProgress {
	started := timeutc.Now()
	pr := RunTableProgress{
		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		StartedAt: &started,

		Keyspace: table.Keyspace,
		Table:    table.Table,

		Host: host,

		TableSize: table.Size,
	}

	if err := w.insertRunTableProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Download progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateRestoreTableProgress(ctx context.Context, pr *RunTableProgress, job *scyllaclient.RcloneJobProgress) {
	startedAt := time.Time(job.StartedAt)
	if !startedAt.IsZero() {
		pr.StartedAt = &startedAt
	}
	pr.Error = job.Error
	pr.Downloaded = job.Uploaded

	if err := w.insertRunTableProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update download progress", "err", err, "pr", pr)
	}
}

func (w *worker) finishRestoreTableProgress(ctx context.Context, pr *RunTableProgress, err error) {
	completedAt := timeutc.Now()
	pr.CompletedAt = &completedAt
	pr.IsRefreshed = err == nil
	if err != nil {
		pr.Error = err.Error()
	}
	if err := w.insertRunTableProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update download progress", "err", err, "pr", pr)
	}
}

func (w *worker) insertRunTableProgress(ctx context.Context, pr *RunTableProgress) error {
	// The main reason for these checks is the ability to 'mock' this function simply by
	// passing empty RunProgress.
	if pr.ClusterID == uuid.Nil || pr.TaskID == uuid.Nil || pr.RunID == uuid.Nil {
		return errors.New("ClusterID, TaskID and RunID can't be empty")
	}
	q := table.One2onerestoreRunTableProgress.InsertQueryContext(ctx, w.managerSession).BindStruct(pr)
	return q.ExecRelease()
}

func (w *worker) insertRunViewProgress(ctx context.Context, pr *RunViewProgress) error {
	// The main reason for these checks is the ability to 'mock' this function simply by
	// passing empty RunProgress.
	if pr.ClusterID == uuid.Nil || pr.TaskID == uuid.Nil || pr.RunID == uuid.Nil {
		return errors.New("ClusterID, TaskID and RunID can't be empty")
	}
	q := table.One2onerestoreRunViewProgress.InsertQueryContext(ctx, w.managerSession).BindStruct(pr)
	return q.ExecRelease()
}

func (w *worker) getProgress(ctx context.Context) (Progress, error) {
	qt := table.One2onerestoreRunTableProgress.SelectQueryContext(ctx, w.managerSession)
	tableIter := qt.BindMap(qb.M{
		"cluster_id": w.runInfo.ClusterID,
		"task_id":    w.runInfo.TaskID,
		"run_id":     w.runInfo.RunID,
	}).Iter()

	qv := table.One2onerestoreRunViewProgress.SelectQueryContext(ctx, w.managerSession)
	viewIter := qv.BindMap(qb.M{
		"cluster_id": w.runInfo.ClusterID,
		"task_id":    w.runInfo.TaskID,
		"run_id":     w.runInfo.RunID,
	}).Iter()

	pr := w.aggregateProgress(tableIter, viewIter)

	var closeErrs error
	if err := tableIter.Close(); err != nil {
		closeErrs = stderr.Join(closeErrs, errors.Wrap(err, "close tables iterator"))
	}
	if err := viewIter.Close(); err != nil {
		closeErrs = stderr.Join(closeErrs, errors.Wrap(err, "close views iterator"))
	}
	return pr, closeErrs
}

type dbIterator interface {
	StructScan(v any) bool
}

func (w *worker) aggregateProgress(tableIter, viewIter dbIterator) Progress {
	var (
		rtp RunTableProgress
		rvp RunViewProgress

		tableProgress = map[scyllaTable]TableProgress{}
		viewProgress  = map[scyllaTable]ViewProgress{}
		result        Progress
	)

	for tableIter.StructScan(&rtp) {
		tableKey := scyllaTable{keyspace: rtp.Keyspace, table: rtp.Table}
		tp := tableProgress[tableKey]
		tp.progress = incrementProgress(tp.progress, progress{
			StartedAt:   rtp.StartedAt,
			CompletedAt: rtp.CompletedAt,
			Size:        rtp.TableSize,
			Restored:    rtp.Downloaded,
			Status:      tableProgressStatus(rtp),
		})
		tp.Keyspace = rtp.Keyspace
		tp.Table = rtp.Table

		tableProgress[tableKey] = tp
		rtp = RunTableProgress{}
	}

	for viewIter.StructScan(&rvp) {
		viewKey := scyllaTable{keyspace: rvp.Keyspace, table: rvp.View}
		vp := viewProgress[viewKey]
		vp.progress = incrementProgress(vp.progress, progress{
			StartedAt:   rvp.StartedAt,
			CompletedAt: rvp.CompletedAt,
			Status:      viewProgressStatus(rvp),
		})
		vp.Keyspace = rvp.Keyspace
		vp.Table = rvp.Table
		vp.View = rvp.View
		vp.ViewType = rvp.ViewType

		viewProgress[viewKey] = vp
		rvp = RunViewProgress{}
	}

	for _, tp := range tableProgress {
		result.Tables = append(result.Tables, tp)
	}

	for _, vp := range viewProgress {
		result.Views = append(result.Views, vp)
	}

	return result
}

func tableProgressStatus(rtp RunTableProgress) ProgressStatus {
	if rtp.Error != "" {
		return ProgressStatusFailed
	}
	if rtp.StartedAt == nil && rtp.CompletedAt == nil {
		return ProgressStatusNotStarted
	}
	if rtp.StartedAt != nil && rtp.CompletedAt != nil && rtp.IsRefreshed {
		return ProgressStatusDone
	}
	return ProgressStatusInProgress
}

func viewProgressStatus(rvp RunViewProgress) ProgressStatus {
	if rvp.Error != "" {
		return ProgressStatusFailed
	}
	switch rvp.ViewBuildStatus {
	case scyllaclient.StatusUnknown:
		return ProgressStatusNotStarted
	case scyllaclient.StatusStarted:
		return ProgressStatusInProgress
	case scyllaclient.StatusSuccess:
		return ProgressStatusDone
	}
	return ProgressStatusInProgress
}

func incrementProgress(dst, src progress) progress {
	dst.Size += src.Size
	dst.Restored += src.Restored
	dst.StartedAt = minTime(dst.StartedAt, src.StartedAt)
	dst.CompletedAt = maxTime(dst.CompletedAt, src.CompletedAt)
	dst.Status = mergeProgressStatus(dst.Status, src.Status)
	return dst
}

// mergeProgressStatus decides which status should table/view progress have.
func mergeProgressStatus(dst, src ProgressStatus) ProgressStatus {
	if dst == "" || dst == src {
		return src
	}
	if dst == ProgressStatusFailed || src == ProgressStatusFailed {
		return ProgressStatusFailed
	}
	return ProgressStatusInProgress
}

func minTime(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}

	if beforeTime(a, b) {
		return a
	}
	return b
}

func maxTime(a, b *time.Time) *time.Time {
	if b == nil {
		return nil
	}
	if beforeTime(a, b) {
		return b
	}
	return a
}

func beforeTime(a, b *time.Time) bool {
	if a == nil {
		return true
	}
	if b == nil {
		return false
	}
	return a.Before(*b)
}
