// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"time"

	stderr "errors"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// initProgress adds entries to RunTableProgress and RunViewProgress, so that API call
// to show progress can return some information at this point.
func (w *worker) initProgress(ctx context.Context, workload []hostWorkload) error {
	tablesToRestore := getTablesToRestore(workload)
	views, err := w.getViews(ctx, tablesToRestore)
	if err != nil {
		return errors.Wrap(err, "get views")
	}
	for _, v := range views {
		if err := w.insertRunViewProgress(ctx, &RunViewProgress{
			ClusterID: w.runInfo.ClusterID,
			TaskID:    w.runInfo.TaskID,
			RunID:     w.runInfo.RunID,

			KeyspaceName: v.Keyspace,
			TableName:    v.View,

			ViewType:        string(v.Type),
			ViewBuildStatus: scyllaclient.StatusUnknown,
		}); err != nil {
			w.logger.Error(ctx, "Failed to init view progress", "err", err)
		}
	}

	for _, work := range workload {
		host := work.host.Addr
		for _, tableInfo := range work.tablesToRestore {
			if err := w.insertRunTableProgress(ctx, &RunTableProgress{
				ClusterID: w.runInfo.ClusterID,
				TaskID:    w.runInfo.TaskID,
				RunID:     w.runInfo.RunID,

				KeyspaceName: tableInfo.keyspace,
				TableName:    tableInfo.table,
				Host:         host,

				TableSize: tableInfo.size,
			}); err != nil {
				w.logger.Error(ctx, "Failed to init table progress", "err", err)
			}
		}
	}

	return nil
}

func (w *worker) reCreateViewProgress(ctx context.Context, view View) *RunViewProgress {
	started := timeutc.Now()
	pr := RunViewProgress{
		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		StartedAt: &started,

		KeyspaceName: view.Keyspace,
		TableName:    view.View,
		ViewType:     string(view.Type),

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

func (w *worker) downloadProgress(ctx context.Context, host string, table backupspec.FilesMeta) *RunTableProgress {
	started := timeutc.Now()
	pr := RunTableProgress{
		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		StartedAt: &started,

		KeyspaceName: table.Keyspace,
		TableName:    table.Table,

		Host: host,

		TableSize: table.Size,
	}

	if err := w.insertRunTableProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Download progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateDownloadProgress(ctx context.Context, pr *RunTableProgress, job *scyllaclient.RcloneJobProgress) {
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

func (w *worker) finishDownloadProgress(ctx context.Context, pr *RunTableProgress, err error) {
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
		tableKey := scyllaTable{keyspace: rtp.KeyspaceName, table: rtp.TableName}
		tp := tableProgress[tableKey]
		tp.progress = incrementProgress(tp.progress, progress{
			StartedAt:   rtp.StartedAt,
			CompletedAt: rtp.CompletedAt,
			Size:        rtp.TableSize,
			Restored:    rtp.Downloaded,
			Status:      tableProgressStatus(rtp),
		})
		tp.Keyspace = rtp.KeyspaceName
		tp.Table = rtp.TableName

		tableProgress[tableKey] = tp
		rtp = RunTableProgress{}
	}

	for viewIter.StructScan(&rvp) {
		viewKey := scyllaTable{keyspace: rvp.KeyspaceName, table: rvp.TableName}
		vp := viewProgress[viewKey]
		vp.progress = incrementProgress(vp.progress, progress{
			StartedAt:   rvp.StartedAt,
			CompletedAt: rvp.CompletedAt,
			Status:      viewProgressStatus(rvp),
		})
		vp.Keyspace = rvp.KeyspaceName
		vp.Table = rvp.TableName
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
	if rtp.StartedAt == nil && rtp.CompletedAt == nil {
		return ProgressStatusNotStarted
	}
	if rtp.StartedAt != nil && rtp.CompletedAt == nil {
		return ProgressStatusInProgress
	}
	if rtp.StartedAt != nil && rtp.CompletedAt != nil && !rtp.IsRefreshed {
		return ProgressStatusInProgress
	}
	if rtp.StartedAt != nil && rtp.CompletedAt != nil && rtp.IsRefreshed {
		return ProgressStatusDone
	}
	return ProgressStatusInProgress
}

func viewProgressStatus(rvp RunViewProgress) ProgressStatus {
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

	if dst.Status == "" {
		dst.Status = src.Status
	}
	if dst.Status == ProgressStatusDone && src.Status != ProgressStatusDone {
		dst.Status = ProgressStatusInProgress
	}
	return dst
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
