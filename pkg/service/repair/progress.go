// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/multierr"
)

// progressManager manages state and progress.
type progressManager interface {
	// Init initializes progress for all tables for all replicas.
	// State from previous run will be used to resume progress.
	Init(ctx context.Context, ttrs []*tableTokenRange) error
	// OnJobResult must be called when worker is done with processing a job.
	// ttrs must contain ranges for a single table.
	// Requires Init() to be called first.
	OnJobResult(ctx context.Context, result jobResult) error
	// OnScyllaJobStart must be called when single job for the repair has started.
	// Job must contain ranges for a single table.
	// Requires Init() to be called first.
	OnScyllaJobStart(ctx context.Context, job job, jobID int32) error
	// OnScyllaJobEnd must be called when single job for the repair is finished.
	// Job must contain ranges for a single table.
	// Must be called after OnScyllaJobStart.
	// Requires Init() to be called first.
	OnScyllaJobEnd(ctx context.Context, job job, jobID int32) error
	// CheckRepaired takes table token range and returns true if it's repaired.
	// Requires Init() to be called first.
	CheckRepaired(ttr *tableTokenRange) bool
}

type progressKey struct {
	host     string
	keyspace string
	table    string
}

type stateKey struct {
	keyspace string
	table    string
}

type dbProgressManager struct {
	logger   log.Logger
	session  gocqlx.Session
	run      *Run
	mu       sync.Mutex
	progress map[progressKey]*RunProgress
	state    map[stateKey]*RunState
}

var _ progressManager = &dbProgressManager{}

func newProgressManager(run *Run, session gocqlx.Session, logger log.Logger) *dbProgressManager {
	return &dbProgressManager{
		logger:   logger.With("run_id", run.ID),
		session:  session,
		run:      run,
		progress: make(map[progressKey]*RunProgress),
		state:    make(map[stateKey]*RunState),
	}
}

func (pm *dbProgressManager) Init(ctx context.Context, ttrs []*tableTokenRange) error {
	if err := pm.restoreState(); err != nil {
		return err
	}

	for _, ttr := range ttrs {
		for _, h := range ttr.Replicas {
			success := int64(0)
			sk := stateKey{
				keyspace: ttr.Keyspace,
				table:    ttr.Table,
			}
			pk := progressKey{
				host:     h,
				keyspace: ttr.Keyspace,
				table:    ttr.Table,
			}
			if state, ok := pm.state[sk]; ok {
				for _, pos := range state.SuccessPos {
					if pos == ttr.Pos {
						success = 1
						break
					}
				}
			}
			if _, ok := pm.progress[pk]; ok {
				pm.progress[pk].TokenRanges++
				pm.progress[pk].Success += success
			} else {
				pm.progress[pk] = &RunProgress{
					ClusterID:   pm.run.ClusterID,
					TaskID:      pm.run.TaskID,
					RunID:       pm.run.ID,
					Host:        h,
					Keyspace:    ttr.Keyspace,
					Table:       ttr.Table,
					TokenRanges: 1,
					Success:     success,
				}
			}
		}
	}

	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()
	for _, p := range pm.progress {
		if err := q.BindStruct(p).Exec(); err != nil {
			return errors.Wrap(err, "init repair progress")
		}
	}

	return nil
}

func (pm *dbProgressManager) restoreState() error {
	if pm.run.PrevID == uuid.Nil {
		return nil
	}
	run := *pm.run
	run.ID = pm.run.PrevID
	states, err := pm.getState(&run)
	if err != nil {
		return err
	}

	q := table.RepairRunState.InsertQuery(pm.session)
	defer q.Release()
	for _, state := range states {
		sk := stateKey{
			keyspace: state.Keyspace,
			table:    state.Table,
		}
		state.RunID = pm.run.ID
		state.ErrorPos = nil
		pm.state[sk] = state
		if err := q.BindStruct(pm.state[sk]).Exec(); err != nil {
			return errors.Wrap(err, "restore previous run state")
		}
	}

	return nil
}

func (pm *dbProgressManager) OnJobResult(ctx context.Context, r jobResult) error {
	ttr := r.Ranges[0]

	pm.logger.Debug(ctx, "OnJobResult", "host", r.job.Host, "keyspace", ttr.Keyspace, "table", ttr.Table, "ranges", len(r.Ranges))

	pm.mu.Lock()
	defer pm.mu.Unlock()

	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()

	for _, h := range ttr.Replicas {
		pk := progressKey{
			host:     h,
			keyspace: ttr.Keyspace,
			table:    ttr.Table,
		}

		if r.Err != nil && !errors.Is(r.Err, errTableDeleted) {
			pm.progress[pk].Error += int64(len(r.Ranges))
		} else {
			pm.progress[pk].Success += int64(len(r.Ranges))
		}

		labels := prometheus.Labels{
			"cluster":  pm.run.clusterName,
			"task":     pm.run.TaskID.String(),
			"keyspace": ttr.Keyspace,
			"host":     r.Host,
		}
		repairSegmentsTotal.With(labels).Set(float64(pm.progress[pk].TokenRanges))
		repairSegmentsSuccess.With(labels).Set(float64(pm.progress[pk].Success))
		repairSegmentsError.With(labels).Set(float64(pm.progress[pk].Error))

		if err := q.BindStruct(pm.progress[pk]).Exec(); err != nil {
			return errors.Wrap(err, "update repair progress")
		}
	}

	sk := stateKey{
		keyspace: ttr.Keyspace,
		table:    ttr.Table,
	}
	if _, ok := pm.state[sk]; ok {
		pm.state[sk].UpdatePositions(r)
	} else {
		rs := &RunState{
			ClusterID: pm.run.ClusterID,
			TaskID:    pm.run.TaskID,
			RunID:     pm.run.ID,
			Keyspace:  ttr.Keyspace,
			Table:     ttr.Table,
		}
		rs.UpdatePositions(r)
		pm.state[sk] = rs
	}

	return table.RepairRunState.InsertQuery(pm.session).BindStruct(pm.state[sk]).ExecRelease()
}

func (pm *dbProgressManager) OnScyllaJobStart(ctx context.Context, job job, jobID int32) error {
	var (
		start = timeutc.Now()
		ttr   = job.Ranges[0]
	)

	pm.logger.Debug(ctx, "OnScyllaJobStart", "host", job.Host, "keyspace", ttr.Keyspace, "table", ttr.Table, "job_id", jobID, "start", start, "ranges", len(job.Ranges))

	for _, h := range ttr.Replicas {
		l := prometheus.Labels{
			"cluster": pm.run.clusterName,
			"task":    pm.run.TaskID.String(),
			"host":    h,
		}
		repairInflightJobs.With(l).Add(1)
		repairInflightTokenRanges.With(l).Add(float64(len(job.Ranges)))
	}

	q := table.RepairJobExecution.InsertQuery(pm.session)
	defer q.Release()
	for _, h := range ttr.Replicas {
		if err := q.BindStruct(pm.newJobExecution(JobIDTuple{job.Host, jobID}, interval{Start: start}, ttr, h)).Exec(); err != nil {
			return errors.Wrap(err, "repair progress")
		}
	}

	return nil
}

func (pm *dbProgressManager) OnScyllaJobEnd(ctx context.Context, job job, jobID int32) error {
	var (
		end = timeutc.Now()
		ttr = job.Ranges[0]
	)

	pm.logger.Debug(ctx, "OnScyllaJobEnd", "host", job.Host, "keyspace", ttr.Keyspace, "table", ttr.Table, "job_id", jobID, "end", end, "ranges", len(job.Ranges))

	for _, h := range ttr.Replicas {
		l := prometheus.Labels{
			"cluster": pm.run.clusterName,
			"task":    pm.run.TaskID.String(),
			"host":    h,
		}
		repairInflightJobs.With(l).Sub(1)
		repairInflightTokenRanges.With(l).Sub(float64(len(job.Ranges)))
	}

	q := table.RepairJobExecution.UpdateQuery(pm.session, "end")
	defer q.Release()
	for _, h := range ttr.Replicas {
		if err := q.BindStruct(pm.newJobExecution(
			JobIDTuple{job.Host, jobID}, interval{End: &end}, ttr, h),
		).Exec(); err != nil {
			return errors.Wrap(err, "update job execution interval")
		}
	}

	return nil
}

func (pm *dbProgressManager) newJobExecution(jobID JobIDTuple, i interval, ttr *tableTokenRange, h string) JobExecution {
	return JobExecution{
		interval:  i,
		ClusterID: pm.run.ClusterID,
		TaskID:    pm.run.TaskID,
		RunID:     pm.run.ID,
		Keyspace:  ttr.Keyspace,
		Table:     ttr.Table,
		Host:      h,
		JobID:     jobID,
	}
}

func (pm *dbProgressManager) CheckRepaired(ttr *tableTokenRange) bool {
	sk := stateKey{
		keyspace: ttr.Keyspace,
		table:    ttr.Table,
	}
	if state, ok := pm.state[sk]; ok {
		for _, pos := range state.SuccessPos {
			if pos == ttr.Pos {
				return true
			}
		}
	}

	return false
}

func (pm *dbProgressManager) getState(run *Run) ([]*RunState, error) {
	q := table.RepairRunState.SelectQuery(pm.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var p []*RunState
	return p, q.SelectRelease(&p)
}

type tableKey struct {
	keyspace string
	table    string
}

type perTableAggregate struct {
	replication int64
	intervals   intervalSlice
	progress    TableProgress
}

func aggregateProgress(intensityFunc func() (float64, int), v ProgressVisitor) (Progress, error) {
	var (
		p        Progress
		perHost  = make(map[string][]TableProgress)
		perTable = make(map[tableKey]*perTableAggregate)
	)

	p.Intensity, p.Parallel = intensityFunc()

	if err := v.ForEach(func(pr *RunProgress, intervals intervalSlice) {
		tp := newTableProgress(pr, intervals)
		// Aggregate per host.
		perHost[pr.Host] = append(perHost[pr.Host], tp)

		// Aggregate by keyspace/table.
		tk := tableKey{
			keyspace: pr.Keyspace,
			table:    pr.Table,
		}
		if _, ok := perTable[tk]; ok {
			// Sum up number of replications so we can get progress numbers
			// across an entire cluster by dividing them up equally.
			perTable[tk].replication++
			perTable[tk].intervals = append(perTable[tk].intervals, intervals...)
			perTable[tk].progress.TokenRanges += pr.TokenRanges
			perTable[tk].progress.Success += pr.Success
			perTable[tk].progress.Error += pr.Error
		} else {
			perTable[tk] = &perTableAggregate{
				replication: 1,
				intervals:   intervals,
				progress:    tp,
			}
		}
	}); err != nil {
		return p, err
	}

	for k, v := range perHost {
		sort.Slice(v, func(i, j int) bool {
			return v[i].Keyspace+v[i].Table < v[j].Keyspace+v[j].Table //nolint:scopelint
		})
		p.Hosts = append(p.Hosts, HostProgress{
			progress: sumTableProgress(v),
			Host:     k,
			Tables:   v,
		})
	}

	sort.Slice(p.Hosts, func(i, j int) bool {
		return p.Hosts[i].Host < p.Hosts[j].Host
	})

	for _, v := range perTable {
		tp := v.progress
		tp.Success /= v.replication
		p.Success += tp.Success
		tp.Error /= v.replication
		p.Error += tp.Error
		tp.TokenRanges /= v.replication
		p.TokenRanges += tp.TokenRanges
		sort.Sort(v.intervals)
		calculateTimestamps(&p.progress, v.progress)

		p.Tables = append(p.Tables, tp)
	}

	// Duration depends on maximum interval ending time, which is bounded by
	// the run completion time, which is dependent on the status of the task.
	// Task may end in error state for example when end interval can't be
	// recorded so we want to bound interval end time by the task completion.
	for i := range p.Tables {
		tk := tableKey{
			keyspace: p.Tables[i].Keyspace,
			table:    p.Tables[i].Table,
		}
		p.Tables[i].Duration = perTable[tk].intervals.Duration(p.Tables[i].CompletedAt).Milliseconds()
		p.Duration += p.Tables[i].Duration
	}

	sort.Slice(p.Tables, func(i, j int) bool {
		return p.Tables[i].Keyspace+p.Tables[i].Table < p.Tables[j].Keyspace+p.Tables[j].Table
	})

	return p, nil
}

func newTableProgress(pr *RunProgress, intervals intervalSlice) TableProgress {
	var completedAt *time.Time
	var duration time.Duration
	if intervals.MinStart() != nil && pr.Success+pr.Error == pr.TokenRanges {
		completedAt = intervals.MaxEnd()
	}
	if intervals.MinStart() != nil {
		duration = intervals.Duration(completedAt)
	}
	tp := TableProgress{
		progress: progress{
			Success:     pr.Success,
			Error:       pr.Error,
			TokenRanges: pr.TokenRanges,
			StartedAt:   intervals.MinStart(),
			CompletedAt: completedAt,
			Duration:    duration.Milliseconds(),
		},
		Keyspace: pr.Keyspace,
		Table:    pr.Table,
	}

	return tp
}

func sumTableProgress(tps []TableProgress) progress {
	p := progress{}
	for i := range tps {
		p.Success += tps[i].Success
		p.Error += tps[i].Error
		p.TokenRanges += tps[i].TokenRanges
		p.Duration += tps[i].Duration

		calculateTimestamps(&p, tps[i])
	}

	return p
}

func calculateTimestamps(p *progress, tp TableProgress) {
	if tp.StartedAt != nil {
		if p.StartedAt == nil {
			p.StartedAt = tp.StartedAt
		} else if tp.StartedAt.Before(*p.StartedAt) {
			p.StartedAt = tp.StartedAt
		}
	}
	if tp.CompletedAt != nil {
		if p.CompletedAt == nil {
			p.CompletedAt = tp.CompletedAt
		} else if tp.CompletedAt.After(*p.CompletedAt) {
			p.CompletedAt = tp.CompletedAt
		}
	}
}

// ProgressVisitor knows how to iterate over list of RunProgress results.
type ProgressVisitor interface {
	ForEach(func(*RunProgress, intervalSlice)) error
}

type progressVisitor struct {
	session gocqlx.Session
	run     *Run
}

// NewProgressVisitor creates new progress iterator.
func NewProgressVisitor(run *Run, session gocqlx.Session) ProgressVisitor {
	return &progressVisitor{
		session: session,
		run:     run,
	}
}

// ForEach iterates over each run progress and runs visit function on it.
// If visit wants to reuse RunProgress it must copy it because memory is reused
// between calls.
func (i *progressVisitor) ForEach(visit func(*RunProgress, intervalSlice)) error {
	iter := table.RepairRunProgress.SelectQuery(i.session).BindMap(qb.M{
		"cluster_id": i.run.ClusterID,
		"task_id":    i.run.TaskID,
		"run_id":     i.run.ID,
	}).Iter()

	pr := &RunProgress{}
	q := table.RepairJobExecution.SelectBuilder("start", "end").
		Where(
			qb.Eq("keyspace_name"),
			qb.Eq("table_name"),
			qb.Eq("host"),
		).Query(i.session)
	defer q.Release()
	for iter.StructScan(pr) {
		var intervals intervalSlice
		if err := q.BindStruct(pr).Select(&intervals); err != nil {
			return multierr.Combine(err, iter.Close())
		}
		visit(pr, intervals)
	}

	return iter.Close()
}
