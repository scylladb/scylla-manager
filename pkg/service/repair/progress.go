// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// progressManager manages state and progress.
type progressManager interface {
	// Init initializes progress for all tables for all replicas.
	// State from previous run will be used to resume progress.
	Init(ctx context.Context, ttrs []*tableTokenRange) error
	// OnStartJob updates timestamp of the RepairRunProgress.
	// It assumes that job contains ranges that target only one table.
	// Requires Init() to be called first.
	OnStartJob(ctx context.Context, job job) error
	// Update updates RunProgress based on jobResult.
	// It assumes that job contains ranges that target only one table.
	// Requires Init() to be called first.
	Update(ctx context.Context, r jobResult) error
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
	progress map[progressKey]*RunProgress
	state    map[stateKey]*RunState
}

var _ progressManager = &dbProgressManager{}

func newProgressManager(run *Run, session gocqlx.Session) *dbProgressManager {
	return &dbProgressManager{
		session:  session,
		run:      run,
		progress: make(map[progressKey]*RunProgress),
		state:    make(map[stateKey]*RunState),
	}
}

func (pm *dbProgressManager) Init(ctx context.Context, ttrs []*tableTokenRange) error {
	if err := pm.restoreState(ctx); err != nil {
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

	for _, p := range pm.progress {
		if err := pm.upsertProgress(ctx, p); err != nil {
			return errors.Wrap(err, "init repair progress")
		}
	}

	return nil
}

func (pm *dbProgressManager) restoreState(ctx context.Context) error {
	if pm.run.PrevID == uuid.Nil {
		return nil
	}
	run := *pm.run
	run.ID = pm.run.PrevID
	states, err := pm.getState(&run)
	if err != nil {
		return err
	}
	for _, state := range states {
		sk := stateKey{
			keyspace: state.Keyspace,
			table:    state.Table,
		}
		state.RunID = pm.run.ID
		state.ErrorPos = nil
		pm.state[sk] = state
		if err := pm.upsertState(ctx, pm.state[sk]); err != nil {
			return err
		}
	}

	return nil
}

func (pm *dbProgressManager) OnStartJob(ctx context.Context, job job) error {
	var (
		ttr = job.Ranges[0]
		now = timeutc.Now()
	)

	for _, h := range ttr.Replicas {
		pk := progressKey{
			host:     h,
			keyspace: ttr.Keyspace,
			table:    ttr.Table,
		}
		if pm.progress[pk].StartedAt != nil {
			continue
		}
		pm.progress[pk].StartedAt = &now
		if err := pm.upsertProgress(ctx, pm.progress[pk]); err != nil {
			return errors.Wrap(err, "init repair progress")
		}
	}

	return nil
}

func (pm *dbProgressManager) Update(ctx context.Context, r jobResult) error {
	ttr := r.Ranges[0]
	now := timeutc.Now()

	for _, h := range ttr.Replicas {
		pk := progressKey{
			host:     h,
			keyspace: ttr.Keyspace,
			table:    ttr.Table,
		}

		if r.Err != nil {
			pm.progress[pk].Error += int64(len(r.Ranges))
		} else {
			pm.progress[pk].Success += int64(len(r.Ranges))
		}

		if pm.progress[pk].Success+pm.progress[pk].Error == pm.progress[pk].TokenRanges {
			pm.progress[pk].CompletedAt = &now
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

		if err := pm.upsertProgress(ctx, pm.progress[pk]); err != nil {
			return errors.Wrap(err, "update repair progress")
		}
	}

	if err := pm.updateState(ctx, r); err != nil {
		return errors.Wrap(err, "save state")
	}

	return nil
}

func (pm *dbProgressManager) updateState(ctx context.Context, r jobResult) error {
	ttr := r.Ranges[0]
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

	return pm.upsertState(ctx, pm.state[sk])
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

// upsertProgress upserts a repair run progress.
func (pm *dbProgressManager) upsertProgress(ctx context.Context, p *RunProgress) error {
	pm.logger.Debug(ctx, "UpsertProgress", "run_progress", p)

	return table.RepairRunProgress.InsertQuery(pm.session).BindStruct(p).ExecRelease()
}

// upsertState upserts a repair state information.
func (pm *dbProgressManager) upsertState(ctx context.Context, s *RunState) error {
	pm.logger.Debug(ctx, "UpsertState", "run_state", s)

	return table.RepairRunState.InsertQuery(pm.session).BindStruct(s).ExecRelease()
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

type progressReplication struct {
	replication int64
	progress    TableProgress
}

func aggregateProgress(intensityFunc func(host string) float64, v ProgressVisitor) (Progress, error) {
	var (
		p        Progress
		perHost  = make(map[string][]TableProgress)
		perTable = make(map[tableKey]*progressReplication)
	)

	if err := v.ForEach(func(pr *RunProgress) {
		// Aggregate per host.
		perHost[pr.Host] = append(perHost[pr.Host], toTableProgress(pr))

		// Aggregate by keyspace/table.
		tk := tableKey{
			keyspace: pr.Keyspace,
			table:    pr.Table,
		}
		if _, ok := perTable[tk]; ok {
			// replication is calculating number of replications so we can
			// divide numbers to get numbers across an entire cluster.
			perTable[tk].replication++
			perTable[tk].progress.TokenRanges += pr.TokenRanges
			perTable[tk].progress.Success += pr.Success
			perTable[tk].progress.Error += pr.Error
		} else {
			perTable[tk] = &progressReplication{
				replication: 1,
				progress:    toTableProgress(pr),
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
			progress:  sumTableProgress(v),
			Host:      k,
			Tables:    v,
			Intensity: intensityFunc(k),
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
		p.Tables = append(p.Tables, tp)

		assignTimestamps(&p.progress, v.progress)
	}

	sort.Slice(p.Tables, func(i, j int) bool {
		return p.Tables[i].Keyspace+p.Tables[i].Table < p.Tables[j].Keyspace+p.Tables[j].Table
	})

	return p, nil
}

func toTableProgress(pr *RunProgress) TableProgress {
	tp := TableProgress{
		progress: progress{
			Success:     pr.Success,
			Error:       pr.Error,
			TokenRanges: pr.TokenRanges,
			StartedAt:   pr.StartedAt,
			CompletedAt: pr.CompletedAt,
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

		assignTimestamps(&p, tps[i])
	}

	return p
}

func assignTimestamps(p *progress, tp TableProgress) {
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
	ForEach(func(*RunProgress)) error
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
func (i *progressVisitor) ForEach(visit func(*RunProgress)) error {
	iter := table.RepairRunProgress.SelectQuery(i.session).BindMap(qb.M{
		"cluster_id": i.run.ClusterID,
		"task_id":    i.run.TaskID,
		"run_id":     i.run.ID,
	}).Iter()

	pr := &RunProgress{}
	for iter.StructScan(pr) {
		visit(pr)
	}

	return iter.Close()
}
