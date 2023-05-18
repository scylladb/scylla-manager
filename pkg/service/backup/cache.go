package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type BackupCache interface {
	clonePrevProgress(run *Run) error
	putRun(r *Run) error
	updateStage(ctx context.Context, run *Run, stage backupspec.Stage)
	putRunProgress(ctx context.Context, p *RunProgress) error
	putValidationRunProgress(p validationRunProgress) error
	resumeUploadProgress(prevRunID uuid.UUID) func(context.Context, *RunProgress)

	GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error)
	GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error)
	GetValidationProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]ValidationHostProgress, error)
	CreateProgressVisitor(run *Run) ProgressVisitor
}

type ScyllaCache struct {
	session gocqlx.Session
	logger  log.Logger
}

func NewScyllaCache(session gocqlx.Session, logger log.Logger) (*ScyllaCache, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}
	return &ScyllaCache{logger: logger, session: session}, nil
}

func (s *ScyllaCache) clonePrevProgress(run *Run) error {
	q := table.BackupRunProgress.InsertQuery(s.session)
	defer q.Release()

	prevRun := &Run{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}
	v := s.CreateProgressVisitor(prevRun)
	return v.ForEach(func(p *RunProgress) error {
		p.RunID = run.ID
		return q.BindStruct(p).Exec()
	})
}

func (s *ScyllaCache) putRun(r *Run) error {
	q := table.BackupRun.InsertQuery(s.session).BindStruct(r)
	return q.ExecRelease()
}

func (s *ScyllaCache) updateStage(ctx context.Context, run *Run, stage backupspec.Stage) {
	run.Stage = stage

	q := table.BackupRun.UpdateQuery(s.session, "stage").BindStruct(run)
	if err := q.ExecRelease(); err != nil {
		s.logger.Error(ctx, "Failed to update run stage", "error", err)
	}
}

func (s *ScyllaCache) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	q := table.BackupRunProgress.InsertQuery(s.session).BindStruct(p)
	return q.ExecRelease()
}

func (s *ScyllaCache) putValidationRunProgress(p validationRunProgress) error {
	return table.ValidateBackupRunProgress.InsertQuery(s.session).BindStruct(p).ExecRelease()
}

func (s *ScyllaCache) resumeUploadProgress(prevRunID uuid.UUID) func(context.Context, *RunProgress) {
	return func(ctx context.Context, p *RunProgress) {
		if prevRunID == uuid.Nil {
			return
		}
		prev := *p
		prev.RunID = prevRunID

		if err := table.BackupRunProgress.GetQuery(s.session).
			BindStruct(prev).
			GetRelease(&prev); err != nil {
			s.logger.Error(ctx, "Failed to get previous progress",
				"cluster_id", p.ClusterID,
				"task_id", p.TaskID,
				"run_id", p.RunID,
				"prev_run_id", prevRunID,
				"table", p.TableName,
				"error", err,
			)
			return
		}

		// Copy size as uploaded files are deleted and size of files on disk is diminished.
		if prev.IsUploaded() {
			p.Size = prev.Size
			p.Uploaded = prev.Uploaded
			p.Skipped = prev.Skipped
		} else {
			diskSize := p.Size
			p.Size = prev.Size
			p.Uploaded = 0
			p.Skipped = prev.Size - diskSize
		}
	}
}

func (s *ScyllaCache) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	q := qb.Select(table.BackupRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(20).Query(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*Run
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	for _, r := range runs {
		// stageNone can be hit when we want to resume a 2.0 backup run
		// this is not supported.
		if r.Stage == backupspec.StageDone || r.Stage == stageNone {
			break
		}
		if r.Stage.Resumable() {
			return r, nil
		}
	}

	return nil, service.ErrNotFound
}

func (s *ScyllaCache) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	q := table.BackupRun.GetQuery(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r Run
	return &r, q.GetRelease(&r)
}

func (s *ScyllaCache) GetValidationProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]ValidationHostProgress, error) {
	s.logger.Debug(ctx, "GetValidationProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	q := table.ValidateBackupRunProgress.SelectQuery(s.session).BindStruct(validationRunProgress{
		ClusterID: clusterID,
		TaskID:    taskID,
		RunID:     runID,
	})
	defer q.Release()

	var result []ValidationHostProgress
	return result, q.Iter().Unsafe().Select(&result)
}

func (s *ScyllaCache) CreateProgressVisitor(run *Run) ProgressVisitor {
	return &scyllaProgressVisitor{
		session: s.session,
		run:     run,
	}
}

// ProgressVisitor knows how to iterate over list of RunProgress results.
type ProgressVisitor interface {
	ForEach(func(*RunProgress) error) error
}

type scyllaProgressVisitor struct {
	session gocqlx.Session
	run     *Run
}

// ForEach iterates over each run progress and runs visit function on it.
// If visit wants to reuse RunProgress it must copy it because memory is reused
// between calls.
func (i *scyllaProgressVisitor) ForEach(visit func(*RunProgress) error) error {
	iter := table.BackupRunProgress.SelectQuery(i.session).BindMap(qb.M{
		"cluster_id": i.run.ClusterID,
		"task_id":    i.run.TaskID,
		"run_id":     i.run.ID,
	}).Iter()

	pr := new(RunProgress)
	for iter.StructScan(pr) {
		if err := visit(pr); err != nil {
			iter.Close()
			return err
		}
	}

	return iter.Close()
}
