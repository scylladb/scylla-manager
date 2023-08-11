// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// progressManager manages state and progress.
type progressManager interface {
	// Init initializes progress for all tables for all hosts.
	Init(plan *plan) error
	// OnJobStart must be called when single repair job is started.
	OnJobStart(ctx context.Context, job job)
	// OnJobEnd must be called when single repair job is finished.
	OnJobEnd(ctx context.Context, job jobResult)
	// UpdatePlan marks plan's ranges which were already successfully
	// repaired in the previous run.
	UpdatePlan(plan *plan)
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
	run     *Run
	session gocqlx.Session
	metrics metrics.RepairMetrics
	logger  log.Logger

	mu       sync.Mutex
	progress map[progressKey]*RunProgress
	state    map[stateKey]*RunState
}

var _ progressManager = &dbProgressManager{}

func (pm *dbProgressManager) Init(plan *plan) error {
	if err := pm.initProgress(plan); err != nil {
		return err
	}
	return pm.initState(plan)
}

func (pm *dbProgressManager) initProgress(plan *plan) error {
	pm.progress = make(map[progressKey]*RunProgress)
	// Fill all possible progress entries (#tables * #nodes)
	for _, kp := range plan.Keyspaces {
		for _, tp := range kp.Tables {
			for _, rep := range kp.Replicas {
				for _, h := range rep.ReplicaSet {
					pk := progressKey{
						host:     h,
						keyspace: kp.Keyspace,
						table:    tp.Table,
					}
					rp := pm.progress[pk]
					if rp == nil {
						rp = &RunProgress{
							ClusterID: pm.run.ClusterID,
							TaskID:    pm.run.TaskID,
							RunID:     pm.run.ID,
							Host:      pk.host,
							Keyspace:  pk.keyspace,
							Table:     pk.table,
						}
						pm.progress[pk] = rp
					}
					rp.TokenRanges += int64(len(rep.Ranges))
				}
			}
		}
	}

	// Update progress entries from previous run
	err := pm.ForEachPrevRunProgress(func(rp *RunProgress) {
		pk := progressKey{
			host:     rp.Host,
			keyspace: rp.Keyspace,
			table:    rp.Table,
		}
		if _, ok := pm.progress[pk]; !ok {
			return
		}
		cp := *rp
		cp.RunID = pm.run.ID
		cp.DurationStartedAt = nil // Reset duration counter from previous run
		cp.Error = 0               // Failed ranges will be retried
		pm.progress[pk] = &cp
	})
	if err != nil {
		return errors.Wrap(err, "fetch progress from previous run")
	}

	// Insert updated progress into DB
	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()
	for _, p := range pm.progress {
		if err := q.BindStruct(p).Exec(); err != nil {
			return errors.Wrap(err, "init repair progress")
		}
		pm.metrics.SetTokenRanges(pm.run.ClusterID, p.Keyspace, p.Table, p.Host,
			p.TokenRanges, p.Success, p.Error)
	}

	return nil
}

func (pm *dbProgressManager) initState(plan *plan) error {
	pm.state = make(map[stateKey]*RunState)
	// Fill all possible state entries (#tables)
	for _, kp := range plan.Keyspaces {
		for _, tp := range kp.Tables {
			sk := stateKey{
				keyspace: kp.Keyspace,
				table:    tp.Table,
			}
			pm.state[sk] = &RunState{
				ClusterID: pm.run.ClusterID,
				TaskID:    pm.run.TaskID,
				RunID:     pm.run.ID,
				Keyspace:  sk.keyspace,
				Table:     sk.table,
			}
		}
	}

	// Update state entries from previous run
	err := pm.ForEachPrevRunState(func(sp *RunState) {
		sk := stateKey{
			keyspace: sp.Keyspace,
			table:    sp.Table,
		}
		if _, ok := pm.state[sk]; !ok {
			return
		}
		cp := *sp
		cp.RunID = pm.run.ID
		pm.state[sk] = &cp
	})
	if err != nil {
		return errors.Wrap(err, "fetch state from previous run")
	}

	// Insert updated state into DB
	q := table.RepairRunState.InsertQuery(pm.session)
	defer q.Release()
	for _, rs := range pm.state {
		if err := q.BindStruct(rs).Exec(); err != nil {
			return errors.Wrap(err, "init repair state")
		}
	}

	return nil
}

// UpdatePlan marks already repaired token ranges in plan using state.
func (pm *dbProgressManager) UpdatePlan(plan *plan) {
	for _, kp := range plan.Keyspaces {
		for tabIdx, tp := range kp.Tables {
			sk := stateKey{
				keyspace: kp.Keyspace,
				table:    tp.Table,
			}
			// Skip only successfully repaired ranges
			for _, r := range pm.state[sk].SuccessRanges {
				if repIdx, ok := kp.TokenRepIdx[r]; ok {
					_ = tp.MarkRange(repIdx, r)
					kp.Tables[tabIdx].Done++
				}
			}
		}
	}
}

func (pm *dbProgressManager) OnJobStart(ctx context.Context, j job) {
	start := timeutc.Now()
	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()

	for _, h := range j.replicaSet {
		pk := progressKey{
			host:     h,
			keyspace: j.keyspace,
			table:    j.table,
		}

		pm.mu.Lock()
		rp := pm.progress[pk]

		rp.runningJobCount++
		if rp.StartedAt == nil {
			rp.StartedAt = &start
		}
		if rp.DurationStartedAt == nil {
			rp.DurationStartedAt = &start
		}

		q.BindStruct(rp)
		pm.mu.Unlock()

		if err := q.Exec(); err != nil {
			pm.logger.Error(ctx, "Update repair progress", "key", pk, "error", err)
		}
		pm.metrics.AddJob(pm.run.ClusterID, h, len(j.ranges))
	}
}

func (pm *dbProgressManager) OnJobEnd(ctx context.Context, result jobResult) {
	pm.onJobEndProgress(ctx, result)
	pm.onJobEndState(ctx, result)
}

func (pm *dbProgressManager) onJobEndProgress(ctx context.Context, result jobResult) {
	end := timeutc.Now()
	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()

	for _, h := range result.replicaSet {
		pk := progressKey{
			host:     h,
			keyspace: result.keyspace,
			table:    result.table,
		}

		pm.mu.Lock()
		rp := pm.progress[pk]

		if result.Success() {
			rp.Success += int64(len(result.ranges))
		} else {
			rp.Error += int64(len(result.ranges))
		}

		rp.runningJobCount--
		if rp.runningJobCount == 0 {
			rp.AddDuration(end)
		}

		if rp.Completed() {
			rp.CompletedAt = &end
		}
		q.BindStruct(rp)

		pm.mu.Unlock()

		if err := q.Exec(); err != nil {
			pm.logger.Error(ctx, "Update repair progress", "key", pk, "error", err)
		}

		pm.metrics.SubJob(pm.run.ClusterID, h, len(result.ranges))
		pm.metrics.SetTokenRanges(pm.run.ClusterID, result.keyspace, result.table, h,
			rp.TokenRanges, rp.Success, rp.Error)
	}
}

func (pm *dbProgressManager) onJobEndState(ctx context.Context, result jobResult) {
	pm.mu.Lock()

	sk := stateKey{
		keyspace: result.keyspace,
		table:    result.table,
	}
	rs := pm.state[sk]

	if result.Success() {
		rs.SuccessRanges = append(rs.SuccessRanges, result.ranges...)
	}

	q := table.RepairRunState.InsertQuery(pm.session).BindStruct(rs)

	pm.mu.Unlock()

	if err := q.ExecRelease(); err != nil {
		pm.logger.Error(ctx, "Update repair state", "key", sk, "error", err)
	}
}

type tableKey struct {
	keyspace string
	table    string
}

func (pm *dbProgressManager) aggregateProgress() (Progress, error) {
	var (
		perHost = make(map[string]HostProgress)
		// As it is impossible to fill table progress solely from repair run progress,
		// its ranges stats are a simple aggregation (with duplication) of perHost ranges.
		// This is not a problem for 'sctool progress', as we only display % progress there,
		// and it is still mostly accurate.
		// Duration is calculated based on StartedAt and CompletedAt
		// in order to avoid aggregation duplication.
		perTable = make(map[tableKey]TableProgress)
		now      = timeutc.Now()
		ancient  = time.Time{}.Add(time.Hour)
	)

	err := pm.ForEachRunProgress(func(rp *RunProgress) {
		prog := extractProgress(rp, now)
		tp := TableProgress{
			progress: prog,
			Keyspace: rp.Keyspace,
			Table:    rp.Table,
		}

		host, ok := perHost[rp.Host]
		if !ok {
			// Set CompletedAt for correct progress merging.
			// It's enough to take some ancient, non-zero time.
			host = HostProgress{
				progress: progress{
					CompletedAt: &ancient,
				},
				Host: rp.Host,
			}
		}
		host.Tables = append(host.Tables, tp)
		host.progress = mergeProgress(host.progress, prog)
		perHost[rp.Host] = host

		tk := tableKey{keyspace: rp.Keyspace, table: rp.Table}
		tab, ok := perTable[tk]
		if !ok {
			tab = TableProgress{
				progress: progress{
					CompletedAt: &ancient,
				},
				Keyspace: rp.Keyspace,
				Table:    rp.Table,
			}
		}
		tab.progress = mergeProgress(tab.progress, prog)
		perTable[tk] = tab
	})
	if err != nil {
		return Progress{}, err
	}

	p := Progress{
		progress: progress{
			CompletedAt: &ancient,
		},
	}

	// Sort host and table progress
	for _, hp := range perHost {
		sort.Slice(hp.Tables, func(i, j int) bool {
			t1 := hp.Tables[i]
			t2 := hp.Tables[j]
			return t1.Keyspace+t1.Table < t2.Keyspace+t2.Table
		})

		p.progress = mergeProgress(p.progress, hp.progress)
		p.Hosts = append(p.Hosts, hp)
	}
	sort.Slice(p.Hosts, func(i, j int) bool {
		return p.Hosts[i].Host < p.Hosts[j].Host
	})

	for _, tp := range perTable {
		tp.Duration = recalculateDuration(tp.progress, now)
		p.Tables = append(p.Tables, tp)
	}
	sort.Slice(p.Tables, func(i, j int) bool {
		t1 := p.Tables[i]
		t2 := p.Tables[j]
		return t1.Keyspace+t1.Table < t2.Keyspace+t2.Table
	})

	if p.CompletedAt == &ancient {
		p.CompletedAt = nil
	}
	p.Duration = recalculateDuration(p.progress, now)
	return p, nil
}

// recalculateDuration returns duration calculated based on
// StartedAt, CompletedAt and now.
func recalculateDuration(pr progress, now time.Time) int64 {
	if !isTimeSet(pr.StartedAt) {
		return 0
	}
	if !isTimeSet(pr.CompletedAt) {
		return now.Sub(*pr.StartedAt).Milliseconds()
	}
	return pr.CompletedAt.Sub(*pr.StartedAt).Milliseconds()
}

func extractProgress(rp *RunProgress, now time.Time) progress {
	return progress{
		Success:     rp.Success,
		Error:       rp.Error,
		TokenRanges: rp.TokenRanges,
		StartedAt:   rp.StartedAt,
		CompletedAt: rp.CompletedAt,
		Duration:    rp.CurrentDuration(now).Milliseconds(),
	}
}

func mergeProgress(pr1, pr2 progress) progress {
	return progress{
		TokenRanges: pr1.TokenRanges + pr2.TokenRanges,
		Success:     pr1.Success + pr2.Success,
		Error:       pr1.Error + pr2.Error,
		StartedAt:   chooseStartedAt(pr1.StartedAt, pr2.StartedAt),
		CompletedAt: chooseCompletedAt(pr1.CompletedAt, pr2.CompletedAt),
		Duration:    pr1.Duration + pr2.Duration,
	}
}

// chooseStartedAt returns earlier time or nil if each is unset.
func chooseStartedAt(sa1, sa2 *time.Time) *time.Time {
	if isTimeSet(sa1) {
		if !isTimeSet(sa2) || sa1.Before(*sa2) {
			return sa1
		}
	}
	return sa2
}

// chooseCompletedAt returns later time or nil if any is unset.
func chooseCompletedAt(ca1, ca2 *time.Time) *time.Time {
	if !isTimeSet(ca1) || !isTimeSet(ca2) {
		return nil
	}
	if ca1.After(*ca2) {
		return ca1
	}
	return ca2
}

func (pm *dbProgressManager) ForEachRunProgress(f func(*RunProgress)) error {
	return pm.forEachRunProgress(false, f)
}

func (pm *dbProgressManager) ForEachPrevRunProgress(f func(*RunProgress)) error {
	return pm.forEachRunProgress(true, f)
}

func (pm *dbProgressManager) forEachRunProgress(prev bool, f func(*RunProgress)) error {
	m := qb.M{
		"cluster_id": pm.run.ClusterID,
		"task_id":    pm.run.TaskID,
		"run_id":     pm.run.ID,
	}
	if prev {
		if pm.run.PrevID == uuid.Nil {
			return nil
		}
		m["run_id"] = pm.run.PrevID
	}
	q := table.RepairRunProgress.SelectQuery(pm.session).BindMap(m)
	defer q.Release()
	iter := q.Iter()

	pr := &RunProgress{}
	for iter.StructScan(pr) {
		f(pr)
	}
	return iter.Close()
}

func (pm *dbProgressManager) ForEachRunState(f func(*RunState)) error {
	return pm.forEachRunState(false, f)
}

func (pm *dbProgressManager) ForEachPrevRunState(f func(*RunState)) error {
	return pm.forEachRunState(true, f)
}

func (pm *dbProgressManager) forEachRunState(prev bool, f func(*RunState)) error {
	m := qb.M{
		"cluster_id": pm.run.ClusterID,
		"task_id":    pm.run.TaskID,
		"run_id":     pm.run.ID,
	}
	if prev {
		if pm.run.PrevID == uuid.Nil {
			return nil
		}
		m["run_id"] = pm.run.PrevID
	}
	q := table.RepairRunState.SelectQuery(pm.session).BindMap(m)
	defer q.Release()
	iter := q.Iter()

	pr := &RunState{}
	for iter.StructScan(pr) {
		f(pr)
	}
	return iter.Close()
}
