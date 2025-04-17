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
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

// ProgressManager manages state and progress.
type ProgressManager interface {
	// Init initializes progress for all tables for all hosts.
	// Specify prevID in order to initialize previous run progress and state.
	Init(plan *plan, prevID uuid.UUID) error
	// GetPrevRun returns latest initialized, uncompleted and fresh run that can be resumed.
	GetPrevRun(ctx context.Context, ageMax time.Duration) *Run
	// OnJobStart must be called when single repair job is started.
	OnJobStart(ctx context.Context, job job)
	// OnJobEnd must be called when single repair job is finished.
	OnJobEnd(ctx context.Context, job jobResult)
	// GetCompletedRanges returns ranges already successfully repaired in the previous runs
	// and the count of all ranges to repair.
	GetCompletedRanges(keyspace, table string) (doneRanges []scyllaclient.TokenRange, allRangesCnt int)
	// AggregateProgress fetches RunProgress from DB and aggregates them into Progress.
	AggregateProgress() (Progress, error)
}

type stateKey struct {
	keyspace string
	table    string
}

type dbProgressManager struct {
	run *Run

	total          atomic.Float64   // Total weighted repair progress
	tableSize      map[string]int64 // Maps table to its size
	totalTableSize int64            // Sum over tableSize
	tableRanges    map[string]int   // Maps table to its range count

	session gocqlx.Session
	metrics metrics.RepairMetrics
	logger  log.Logger

	mu       sync.Mutex
	progress map[scyllaclient.HostKeyspaceTable]*RunProgress
	state    map[stateKey]*RunState
}

var _ ProgressManager = &dbProgressManager{}

func NewDBProgressManager(run *Run, session gocqlx.Session, metrics metrics.RepairMetrics, logger log.Logger) ProgressManager {
	return &dbProgressManager{
		run:     run,
		session: session,
		metrics: metrics,
		logger:  logger.Named("progress"),
	}
}

func (pm *dbProgressManager) Init(plan *plan, prevID uuid.UUID) error {
	pm.run.PrevID = prevID
	pm.total.Store(0)
	pm.metrics.SetProgress(pm.run.ClusterID, 0)

	pm.tableSize = make(map[string]int64)
	pm.totalTableSize = 0
	pm.tableRanges = make(map[string]int)
	for _, ksp := range plan.Keyspaces {
		for _, tp := range ksp.Tables {
			fullName := ksp.Keyspace + "." + tp.Table
			pm.tableSize[fullName] = tp.Size
			pm.totalTableSize += tp.Size
			pm.tableRanges[fullName] = tp.RangesCnt
		}
	}

	// Init state before progress, so that we don't resume progress when state is empty
	if err := pm.initState(plan); err != nil {
		return err
	}
	return pm.initProgress(plan)
}

func (pm *dbProgressManager) GetPrevRun(ctx context.Context, ageMax time.Duration) *Run {
	q := qb.Select(table.RepairRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(20).Query(pm.session).BindMap(qb.M{
		"cluster_id": pm.run.ClusterID,
		"task_id":    pm.run.TaskID,
	})

	var runs []*Run
	if err := q.SelectRelease(&runs); err != nil {
		pm.logger.Info(ctx, "Couldn't query previous run", "error", err)
		return nil
	}

	var prev *Run
	for _, r := range runs {
		if pm.isRunInitialized(r) {
			prev = r
			break
		}
	}
	if prev == nil {
		return nil
	}

	pm.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
	if !prev.EndTime.IsZero() {
		pm.logger.Info(ctx, "Starting from scratch: previous run is completed")
		return nil
	}
	if ageMax > 0 && timeutc.Since(prev.StartTime) > ageMax {
		pm.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	}

	return prev
}

func (pm *dbProgressManager) isRunInitialized(run *Run) bool {
	q := qb.Select(table.RepairRunProgress.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
		qb.Eq("run_id"),
	).Limit(1).Query(pm.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var check []*RunProgress
	err := q.SelectRelease(&check)
	return err == nil && len(check) > 0
}

func (pm *dbProgressManager) initProgress(plan *plan) error {
	pm.progress = make(map[scyllaclient.HostKeyspaceTable]*RunProgress)
	// Fill all possible progress entries (#tables * #nodes)
	for _, kp := range plan.Keyspaces {
		for _, tp := range kp.Tables {
			for _, h := range plan.Hosts {
				pk := newHostKsTable(h, kp.Keyspace, tp.Table)
				pm.progress[newHostKsTable(h, kp.Keyspace, tp.Table)] = &RunProgress{
					ClusterID:   pm.run.ClusterID,
					TaskID:      pm.run.TaskID,
					RunID:       pm.run.ID,
					Host:        pk.Host,
					Keyspace:    pk.Keyspace,
					Table:       pk.Table,
					Size:        plan.Stats[pk].Size,
					TokenRanges: int64(plan.Stats[pk].Ranges),
				}
			}
		}
	}

	// Update progress entries from previous run.
	// Watch out for empty state after upgrade (#3534).
	if !pm.emptyState() {
		err := pm.ForEachPrevRunProgress(func(rp *RunProgress) {
			pk := newHostKsTable(rp.Host, rp.Keyspace, rp.Table)
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
		pm.updateTotalProgress(cp.Keyspace, cp.Table, len(cp.SuccessRanges))
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

func (pm *dbProgressManager) emptyState() bool {
	for _, rs := range pm.state {
		if len(rs.SuccessRanges) > 0 {
			return false
		}
	}
	return true
}

func (pm *dbProgressManager) GetCompletedRanges(keyspace, table string) (doneRanges []scyllaclient.TokenRange, allRangesCnt int) {
	sk := stateKey{
		keyspace: keyspace,
		table:    table,
	}
	return pm.state[sk].SuccessRanges, pm.tableRanges[keyspace+"."+table]
}

func (pm *dbProgressManager) OnJobStart(ctx context.Context, j job) {
	start := timeutc.Now()
	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()

	for _, h := range j.replicaSet {
		pk := newHostKsTable(h.String(), j.keyspace, j.table)

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
		// Inserts of repair run progress don't need to be done under mutex
		// as long as the 1 job per 1 host rule is respected.
		pm.mu.Unlock()

		if err := q.Exec(); err != nil {
			pm.logger.Error(ctx, "Update repair progress", "key", pk, "error", err)
		}
		pm.metrics.AddJob(pm.run.ClusterID, h.String(), len(j.ranges))
	}
}

func (pm *dbProgressManager) OnJobEnd(ctx context.Context, result jobResult) {
	pm.updateTotalProgress(result.keyspace, result.table, len(result.ranges))
	pm.onJobEndProgress(ctx, result)
	pm.onJobEndState(ctx, result)
}

func (pm *dbProgressManager) onJobEndProgress(ctx context.Context, result jobResult) {
	end := timeutc.Now()
	q := table.RepairRunProgress.InsertQuery(pm.session)
	defer q.Release()

	for _, h := range result.replicaSet {
		pm.mu.Lock()

		pk := newHostKsTable(h.String(), result.keyspace, result.table)
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
		// Inserts of repair run progress don't need to be done under mutex
		// as long as the 1 job per 1 host rule is respected.
		pm.mu.Unlock()

		if err := q.Exec(); err != nil {
			pm.logger.Error(ctx, "Update repair progress", "key", pk, "error", err)
		}

		pm.metrics.SubJob(pm.run.ClusterID, h.String(), len(result.ranges))
		pm.metrics.SetTokenRanges(pm.run.ClusterID, result.keyspace, result.table, h.String(),
			rp.TokenRanges, rp.Success, rp.Error)
	}
}

func (pm *dbProgressManager) onJobEndState(ctx context.Context, result jobResult) {
	// Repair state preserves only successfully repaired ranges (failures are a part of repair progress)
	if !result.Success() {
		return
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	sk := stateKey{
		keyspace: result.keyspace,
		table:    result.table,
	}
	rs := pm.state[sk]
	rs.SuccessRanges = append(rs.SuccessRanges, result.ranges...)

	if err := table.RepairRunState.InsertQuery(pm.session).BindStruct(rs).ExecRelease(); err != nil {
		pm.logger.Error(ctx, "Update repair state", "key", sk, "error", err)
	}
}

// updateTotalProgress updates total repair progress weighted by repaired table size.
// It is done by calculating progress delta instead of doing it from scratch.
// This approach is faster than iterating over all RunProgress (#nodes * #tables)
// under mutex, but it also gives more room for float rounding errors.
// However, rounding errors can't be totally avoided even
// when calculating total progress from scratch, so it should be fine.
func (pm *dbProgressManager) updateTotalProgress(keyspace, table string, ranges int) {
	var weight, totalWeight int64
	if pm.totalTableSize == 0 {
		weight = 1
		totalWeight = int64(len(pm.state))
	} else {
		weight = pm.tableSize[keyspace+"."+table]
		totalWeight = pm.totalTableSize
	}

	delta := float64(ranges) / float64(pm.tableRanges[keyspace]) * 100 // Not weighted percentage progress delta
	delta *= float64(weight) / float64(totalWeight)                    // Apply weight

	// Watch out for rounding over 100% errors
	if total := pm.total.Add(delta); total <= 100 {
		pm.metrics.AddProgress(pm.run.ClusterID, delta)
	}
}

type tableKey struct {
	keyspace string
	table    string
}

func (pm *dbProgressManager) AggregateProgress() (Progress, error) {
	var (
		perHost = make(map[string]HostProgress)
		// As it is impossible to fill table progress solely from repair run progress,
		// its ranges stats are a simple aggregation (with duplication) of perHost ranges.
		// This is not a problem for 'sctool progress', as we only display % progress there,
		// and it is still mostly accurate.
		// Duration is calculated based on StartedAt and CompletedAt
		// in order to avoid aggregation duplication.
		perTable  = make(map[tableKey]TableProgress)
		tableSize = make(map[tableKey]int64)
		totalSize int64
		now       = timeutc.Now()
		ancient   = time.Time{}.Add(time.Hour)
	)

	err := pm.ForEachRunProgress(func(rp *RunProgress) {
		tableSize[tableKey{
			keyspace: rp.Keyspace,
			table:    rp.Table,
		}] += rp.Size
		totalSize += rp.Size

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

	if p.TokenRanges == 0 {
		p.SuccessPercentage = -1
		p.ErrorPercentage = -1
		return p, nil
	}

	var weight func(key tableKey) int64
	var totalWeight int64
	if totalSize == 0 {
		weight = func(_ tableKey) int64 {
			return 1
		}
		totalWeight = int64(len(perTable))
	} else {
		weight = func(key tableKey) int64 {
			return tableSize[key]
		}
		totalWeight = totalSize
	}

	successPr, errorPr := calcTotalProgress(perTable, weight, totalWeight)
	p.SuccessPercentage = int(successPr)
	p.ErrorPercentage = int(errorPr)
	return p, nil
}

func calcTotalProgress(perTable map[tableKey]TableProgress, weight func(tableKey) int64, totalWeight int64) (successPr, errorPr float64) {
	if totalWeight == 0 {
		return -1, -1
	}

	for tk, tp := range perTable {
		successPr += float64(tp.Success) / float64(tp.TokenRanges) * float64(weight(tk))
		errorPr += float64(tp.Error) / float64(tp.TokenRanges) * float64(weight(tk))
	}

	successPr = successPr * 100 / float64(totalWeight)
	errorPr = errorPr * 100 / float64(totalWeight)

	return successPr, errorPr
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
