package backup

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"xorm.io/xorm"
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

	insertRun(ctx context.Context, run *RestoreRun)
	insertRunProgress(ctx context.Context, pr *RestoreRunProgress)
	deleteRunProgress(ctx context.Context, pr *RestoreRunProgress)
	clonePrevRestoreProgress(ctx context.Context, run *RestoreRun)

	ForEachRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress))
	GetRestoreRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error)
	ForEachTableRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress))
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

func (s *ScyllaCache) insertRun(ctx context.Context, run *RestoreRun) {
	if err := table.RestoreRun.InsertQuery(s.session).BindStruct(run).ExecRelease(); err != nil {
		s.logger.Error(ctx, "Insert run",
			"run", *run,
			"error", err,
		)
	}
}

func (s *ScyllaCache) insertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(s.session).BindStruct(pr).ExecRelease(); err != nil {
		s.logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (s *ScyllaCache) deleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(s.session).BindStruct(pr).ExecRelease(); err != nil {
		s.logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (s *ScyllaCache) clonePrevRestoreProgress(ctx context.Context, run *RestoreRun) {
	q := table.RestoreRunProgress.InsertQuery(s.session)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	s.ForEachRestoreProgress(ctx, prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID

		if err := q.BindStruct(pr).Exec(); err != nil {
			s.logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})

	s.logger.Info(ctx, "Run after decoration", "run", *run)
}

func (s *ScyllaCache) ForEachRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	iter := table.RestoreRunProgress.SelectQuery(s.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			s.logger.Error(ctx, "Error while iterating over run progress",
				"cluster_id", run.ClusterID,
				"task_id", run.TaskID,
				"run_id", run.ID,
				"error", err,
			)
		}
	}()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}

func (s *ScyllaCache) GetRestoreRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	s.logger.Debug(ctx, "Get run",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	var q *gocqlx.Queryx
	if runID != uuid.Nil {
		q = table.RestoreRun.GetQuery(s.session).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"id":         runID,
		})
	} else {
		q = table.RestoreRun.SelectQuery(s.session).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
		})
	}

	var r RestoreRun
	return &r, q.GetRelease(&r)
}

func (s *ScyllaCache) ForEachTableRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	iter := qb.Select(table.RestoreRunProgress.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
		qb.Eq("run_id"),
		qb.Eq("manifest_path"),
		qb.Eq("keyspace_name"),
		qb.Eq("table_name"),
	).Query(s.session).BindMap(qb.M{
		"cluster_id":    run.ClusterID,
		"task_id":       run.TaskID,
		"run_id":        run.ID,
		"manifest_path": run.ManifestPath,
		"keyspace_name": run.Keyspace,
		"table_name":    run.Table,
	}).Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			s.logger.Error(ctx, "Error while iterating over table's run progress",
				"cluster_id", run.ClusterID,
				"task_id", run.TaskID,
				"run_id", run.ID,
				"manifest_path", run.ManifestPath,
				"keyspace", run.Keyspace,
				"table", run.Table,
				"error", err,
			)
		}
	}()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}

type SQLiteCache struct {
	engine *xorm.Engine
	logger log.Logger
}

func NewSQLiteCache(engine *xorm.Engine, logger log.Logger) (*SQLiteCache, error) {
	cache := &SQLiteCache{logger: logger, engine: engine}
	if err := cache.initCache(); err != nil {
		return nil, err
	}
	return cache, nil
}

func (s *SQLiteCache) initCache() error {
	if err := s.engine.CreateTables(Run{}); err != nil {
		return err
	}
	if err := s.engine.CreateTables(RunProgress{}); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteCache) clonePrevProgress(run *Run) error {
	prevRun := &Run{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}
	v := &sqliteProgressVisitor{
		run:    prevRun,
		engine: s.engine,
	}
	return v.ForEach(func(p *RunProgress) error {
		p.RunID = run.ID
		_, err := s.engine.Insert(p)
		return err
	})
}

func (s *SQLiteCache) putRun(r *Run) error {
	r.PK = r.ClusterID.String() + r.TaskID.String() + r.ID.String()
	existing := Run{
		PK: r.PK,
	}
	shouldUpdate, err := s.engine.Get(&existing)
	if err != nil {
		return err
	}
	if shouldUpdate {
		_, err := s.engine.Update(r)
		return err
	}
	_, err = s.engine.Insert(r)
	return err
}

func (s *SQLiteCache) updateStage(ctx context.Context, run *Run, stage backupspec.Stage) {
	run.PK = run.ClusterID.String() + run.TaskID.String() + run.ID.String()
	run.Stage = stage
	if _, err := s.engine.Update(run, &Run{PK: run.PK}); err != nil {
		s.logger.Error(ctx, "Failed to update run stage", "error", err)
	}
}

func (s *SQLiteCache) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)
	p.PK = fmt.Sprintf("%s%s%s%s%d%s", p.ClusterID.String(), p.TaskID.String(), p.RunID.String(), p.Host, p.Unit, p.TableName)
	existing := RunProgress{
		PK: p.PK,
	}
	shouldUpdate, err := s.engine.Get(&existing)
	if err != nil {
		return err
	}
	if shouldUpdate {
		_, err = s.engine.Update(p, &RunProgress{PK: p.PK})
		return err

	}
	_, err = s.engine.Insert(p)
	return err
}

func (s *SQLiteCache) putValidationRunProgress(p validationRunProgress) error {
	_, err := s.engine.Insert(p)
	return err
}

func (s *SQLiteCache) resumeUploadProgress(prevRunID uuid.UUID) func(context.Context, *RunProgress) {
	return func(ctx context.Context, p *RunProgress) {
		if prevRunID == uuid.Nil {
			return
		}
		var prev RunProgress
		if _, err := s.engine.Get(&prev, &RunProgress{RunID: prevRunID}); err != nil {
			s.logger.Error(ctx, "Failed to get previous progress",
				"cluster_id", p.ClusterID,
				"task_id", p.TaskID,
				"run_id", p.RunID,
				"prev_run_id", prevRunID,
				"table", p.TableName,
				"error", err,
			)
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

func (s *SQLiteCache) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	const stageNone backupspec.Stage = ""
	var runs []*Run

	if err := s.engine.Find(&runs, &Run{ClusterID: clusterID, TaskID: taskID}); err != nil {
		return nil, err
	}
	for _, r := range runs {
		if r.Stage == backupspec.StageDone || r.Stage == stageNone {
			break
		}
		if r.Stage.Resumable() {
			return r, nil
		}
	}

	return nil, service.ErrNotFound
}

func (s *SQLiteCache) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)
	var run Run
	if _, err := s.engine.Get(&run, &Run{ClusterID: clusterID, TaskID: taskID, ID: runID}); err != nil {
		return nil, err
	}

	return &run, nil
}

func (s *SQLiteCache) GetValidationProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]ValidationHostProgress, error) {
	s.logger.Debug(ctx, "GetValidationProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)
	var runProgress []validationRunProgress
	if err := s.engine.Find(&runProgress, &validationRunProgress{ClusterID: clusterID, TaskID: taskID, RunID: runID}); err != nil {
		return nil, err
	}

	var result []ValidationHostProgress
	for _, rp := range runProgress {
		result = append(result, ValidationHostProgress{
			DC:               rp.DC,
			Host:             rp.Host,
			Location:         rp.Location,
			Manifests:        rp.Manifests,
			StartedAt:        rp.StartedAt,
			CompletedAt:      rp.CompletedAt,
			ValidationResult: rp.ValidationResult,
		})
	}
	return result, nil
}

func (s *SQLiteCache) CreateProgressVisitor(run *Run) ProgressVisitor {
	return &sqliteProgressVisitor{
		engine: s.engine,
		run:    run,
	}
}

func (s *SQLiteCache) insertRun(ctx context.Context, run *RestoreRun) {
	if _, err := s.engine.Insert(run); err != nil {
		s.logger.Error(ctx, "Insert run",
			"run", *run,
			"error", err,
		)
	}
}

func (s *SQLiteCache) insertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if _, err := s.engine.Insert(pr); err != nil {
		s.logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (s *SQLiteCache) deleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if _, err := s.engine.Delete(pr); err != nil {
		s.logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (s *SQLiteCache) clonePrevRestoreProgress(ctx context.Context, run *RestoreRun) {
	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	s.ForEachRestoreProgress(ctx, prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID

		if _, err := s.engine.Insert(pr); err != nil {
			s.logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})

	s.logger.Info(ctx, "Run after decoration", "run", *run)
}

func (s *SQLiteCache) ForEachRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	var restoreRunProgresses []RestoreRunProgress
	if err := s.engine.Find(&restoreRunProgresses, &RestoreRunProgress{ClusterID: run.ClusterID, TaskID: run.TaskID, RunID: run.ID}); err != nil {
		s.logger.Error(ctx, "cannot find restoreRunProgresses", "run", run, "err", err)
	}

	for _, rrp := range restoreRunProgresses {
		cb(&rrp)
	}
}

func (s *SQLiteCache) GetRestoreRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	s.logger.Debug(ctx, "Get run",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	var (
		err error
		r   RestoreRun
	)
	if runID != uuid.Nil {
		_, err = s.engine.Get(&r, &RestoreRun{ClusterID: clusterID, TaskID: taskID, ID: runID})
	} else {
		_, err = s.engine.Get(&r, &RestoreRun{ClusterID: clusterID, TaskID: taskID})
	}

	return &r, err
}

func (s *SQLiteCache) ForEachTableRestoreProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	var restoreRunProgresses []RestoreRunProgress
	err := s.engine.Find(restoreRunProgresses, &RestoreRunProgress{
		ClusterID:    run.ClusterID,
		TaskID:       run.TaskID,
		RunID:        run.ID,
		ManifestPath: run.ManifestPath,
		Keyspace:     run.Keyspace,
		Table:        run.Table,
	})
	if err != nil {
		s.logger.Error(ctx, "cannot find restoreRunProgresses", "err", err, "run", run)
	}

	for _, rrp := range restoreRunProgresses {
		cb(&rrp)
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

type sqliteProgressVisitor struct {
	run    *Run
	engine *xorm.Engine
}

func (i *sqliteProgressVisitor) ForEach(visit func(*RunProgress) error) error {
	var progresses []*RunProgress
	if err := i.engine.Find(&progresses, &RunProgress{ClusterID: i.run.ClusterID, TaskID: i.run.TaskID, RunID: i.run.ID}); err != nil {
		return err
	}

	for _, pr := range progresses {
		if err := visit(pr); err != nil {
			return err
		}
	}

	return nil
}
