// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func (w *worker) alterTGCProgress(ctx context.Context, table scyllaTable, mode tombstoneGCMode) *RunProgress {
	started := timeutc.Now()
	pr := RunProgress{
		Stage: StageAlterTGC,

		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		KeyspaceName: table.keyspace,
		TableName:    table.table,
		TombstoneGC:  string(mode),
		StartedAt:    &started,
	}

	if err := w.insertRunProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Alter tombstone_gc mode progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateTGCProgress(ctx context.Context, pr *RunProgress, mode tombstoneGCMode, completedAt time.Time) {
	pr.CompletedAt = &completedAt
	pr.TombstoneGC = string(mode)

	if err := w.insertRunProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update alter tombstone_gc mode progress", "err", err, "pr", pr)
	}
}

func (w *worker) dropViewProgress(ctx context.Context, view View) *RunProgress {
	started := timeutc.Now()
	pr := RunProgress{
		Stage: StageDropViews,

		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		KeyspaceName:    view.Keyspace,
		TableName:       view.BaseTable,
		ViewName:        view.View,
		ViewType:        view.Type,
		ViewBuildStatus: view.BuildStatus,
		StartedAt:       &started,
	}

	if err := w.insertRunProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "DropView progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateDropViewProgress(ctx context.Context, pr *RunProgress, completedAt time.Time) {
	pr.CompletedAt = &completedAt
	if err := w.insertRunProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update dropView progress", "err", err, "pr", pr)
	}
}

func (w *worker) reCreateViewProgress(ctx context.Context, view View) *RunProgress {
	started := timeutc.Now()
	pr := RunProgress{
		Stage: StageRecreateViews,

		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		KeyspaceName:    view.Keyspace,
		TableName:       view.BaseTable,
		ViewName:        view.View,
		ViewType:        view.Type,
		ViewBuildStatus: view.BuildStatus,
		StartedAt:       &started,
	}

	if err := w.insertRunProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Recreate view progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateReCreateViewProgress(ctx context.Context, pr *RunProgress, status scyllaclient.ViewBuildStatus) {
	completedAt := timeutc.Now()
	if status == scyllaclient.StatusSuccess {
		pr.CompletedAt = &completedAt
	}
	pr.ViewBuildStatus = status
	if err := w.insertRunProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update recreate view progress", "err", err, "pr", pr)
	}
}

func (w *worker) downloadProgress(ctx context.Context, remoteDir, host string, shardCount int, jobID int64, table backupspec.FilesMeta) *RunProgress {
	started := timeutc.Now()
	pr := RunProgress{
		Stage: StageData,

		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		KeyspaceName:     table.Keyspace,
		TableName:        table.Table,
		RemoteSSTableDir: remoteDir,
		AgentJobID:       jobID,
		Host:             host,
		ShardCnt:         shardCount,
		TableSize:        table.Size,
		StartedAt:        &started,
	}

	if err := w.insertRunProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Download progress", "err", err, "pr", pr)
	}
	return &pr
}

func (w *worker) updateDownloadProgress(ctx context.Context, pr *RunProgress, job *scyllaclient.RcloneJobProgress) {
	startedAt, completedAt := time.Time(job.StartedAt), time.Time(job.CompletedAt)
	if !startedAt.IsZero() {
		pr.StartedAt = &startedAt
	}
	if !completedAt.IsZero() {
		pr.CompletedAt = &completedAt
	}
	pr.Error = job.Error
	pr.Downloaded = job.Uploaded
	pr.Skipped = job.Skipped
	pr.Failed = job.Failed

	if err := w.insertRunProgress(ctx, pr); err != nil {
		w.logger.Error(ctx, "Update download progress", "err", err, "pr", pr)
	}
}

func (w *worker) progressDone(ctx context.Context, start, end time.Time) {
	pr := RunProgress{
		Stage: StageDone,

		ClusterID: w.runInfo.ClusterID,
		TaskID:    w.runInfo.TaskID,
		RunID:     w.runInfo.RunID,

		StartedAt:   &start,
		CompletedAt: &end,
	}
	if err := w.insertRunProgress(ctx, &pr); err != nil {
		w.logger.Error(ctx, "Done progress", "err", err, "pr", pr)
	}
}

func (w *worker) insertRunProgress(ctx context.Context, pr *RunProgress) error {
	// The main reason for these checks is the ability to 'mock' this function simply by
	// passing empty RunProgress.
	if pr.ClusterID == uuid.Nil || pr.TaskID == uuid.Nil || pr.RunID == uuid.Nil {
		return errors.New("ClusterID, TaskID and RunID can't be empty")
	}
	q := table.One2onerestoreRunProgress.InsertQueryContext(ctx, w.managerSession).BindStruct(pr)
	return q.ExecRelease()
}

func (w *worker) getProgress(ctx context.Context, target Target) (Progress, error) {
	q := table.One2onerestoreRunProgress.SelectQueryContext(ctx, w.managerSession)
	iter := q.BindMap(qb.M{
		"cluster_id": w.runInfo.ClusterID,
		"task_id":    w.runInfo.TaskID,
		"run_id":     w.runInfo.RunID,
	}).Iter()

	pr := w.aggregateProgress(iter, target.SnapshotTag)

	if err := iter.Close(); err != nil {
		return Progress{}, errors.Wrap(err, "close iterator")
	}
	return pr, nil
}

type dbIterator interface {
	StructScan(v any) bool
}

func (w *worker) aggregateProgress(iter dbIterator, snapshotTag string) Progress {
	var (
		tablesProgress = map[scyllaTable]TableProgress{}
		hostsProgress  = map[string]HostProgress{}
		views          []View
		pr             RunProgress
		latestRP       RunProgress
		resultProgress progress
	)

	for iter.StructScan(&pr) {
		latestRP = latestStage(latestRP, pr)
		resultProgress = incrementProgress(resultProgress, toProgress(pr))
		switch pr.Stage {
		case StageData:
			tKey := scyllaTable{keyspace: pr.KeyspaceName, table: pr.TableName}
			tp := tablesProgress[tKey]
			tp.Table = pr.TableName
			tp.Error += pr.Error
			tp.progress = incrementProgress(tp.progress, toProgress(pr))
			tablesProgress[tKey] = tp

			hp := hostsProgress[pr.Host]
			hp.Host = pr.Host
			hp.ShardCnt = pr.ShardCnt
			hp.DownloadedBytes += pr.Downloaded
			hp.DownloadDuration += durationMiliseconds(pr.StartedAt, pr.CompletedAt)
			hostsProgress[pr.Host] = hp
		case StageAlterTGC:
			tKey := scyllaTable{keyspace: pr.KeyspaceName, table: pr.TableName}
			tp := tablesProgress[tKey]
			tp.progress = incrementProgress(tp.progress, toProgress(pr))
			tp.Table = pr.TableName
			tp.TombstoneGC = tombstoneGCMode(pr.TombstoneGC)
			tablesProgress[tKey] = tp
		case StageDropViews, StageRecreateViews:
			views = append(views, View{
				Keyspace:    pr.KeyspaceName,
				View:        pr.ViewName,
				Type:        pr.ViewType,
				BaseTable:   pr.TableName,
				BuildStatus: pr.ViewBuildStatus,
			})
		}
	}

	ksProgress := map[string]KeyspaceProgress{}
	for key, tp := range tablesProgress {
		kp := ksProgress[key.keyspace]
		kp.Keyspace = key.keyspace
		kp.progress = incrementProgress(kp.progress, tp.progress)
		kp.Tables = append(kp.Tables, tp)
		ksProgress[key.keyspace] = kp
	}

	return Progress{
		progress: resultProgress,

		Stage:       latestRP.Stage,
		SnapshotTag: snapshotTag,
		Keyspaces:   slices.Collect(maps.Values(ksProgress)),
		Hosts:       slices.Collect(maps.Values(hostsProgress)),
		Views:       views,
	}
}

func toProgress(rp RunProgress) progress {
	return progress{
		Size:        rp.TableSize,
		Restored:    rp.Downloaded - rp.Failed - rp.Skipped,
		Downloaded:  rp.Downloaded,
		Failed:      rp.Failed,
		StartedAt:   rp.StartedAt,
		CompletedAt: rp.CompletedAt,
	}
}

func incrementProgress(dst, src progress) progress {
	dst.Size += src.Size
	dst.Downloaded += src.Downloaded
	dst.Restored += src.Restored
	dst.Failed += src.Failed
	dst.StartedAt = minTime(dst.StartedAt, src.StartedAt)
	dst.CompletedAt = maxTime(dst.CompletedAt, src.CompletedAt)
	return dst
}

func latestStage(a, b RunProgress) RunProgress {
	if a.Stage == StageDone {
		return a
	}
	if b.Stage == StageDone {
		return b
	}
	if beforeTime(a.StartedAt, b.StartedAt) {
		return b
	}
	return a
}

func durationMiliseconds(start, end *time.Time) int64 {
	if start == nil || end == nil {
		return 0
	}
	return end.Sub(*start).Milliseconds()
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
