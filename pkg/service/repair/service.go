// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/dcfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

// Service orchestrates cluster repairs.
type Service struct {
	session gocqlx.Session
	config  Config
	metrics metrics.RepairMetrics

	scyllaClient   scyllaclient.ProviderFunc
	clusterSession cluster.SessionFunc
	logger         log.Logger

	intensityHandlers map[uuid.UUID]*intensityHandler
	mu                sync.Mutex
}

func NewService(session gocqlx.Session, config Config, metrics metrics.RepairMetrics,
	scyllaClient scyllaclient.ProviderFunc, clusterSession cluster.SessionFunc, logger log.Logger,
) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session:           session,
		config:            config,
		metrics:           metrics,
		scyllaClient:      scyllaClient,
		clusterSession:    clusterSession,
		logger:            logger,
		intensityHandlers: make(map[uuid.UUID]*intensityHandler),
	}, nil
}

// Runner creates a Runner that handles repairs.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}

// ErrEmptyRepair is returned when there is nothing to repair (e.g. repaired keyspaces are not replicated).
var ErrEmptyRepair = errors.New("nothing to repair")

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (Target, error) {
	p := defaultTaskProperties()

	// Parse task properties
	if err := json.Unmarshal(properties, &p); err != nil {
		return Target{}, service.ErrValidate(errors.Wrapf(err, "parse runner properties: %s", properties))
	}

	// Copy basic properties
	t := Target{
		Host:                p.Host,
		FailFast:            p.FailFast,
		Continue:            p.Continue,
		Intensity:           p.Intensity,
		Parallel:            p.Parallel,
		SmallTableThreshold: p.SmallTableThreshold,
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrapf(err, "get client")
	}

	// Get hosts in DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return t, errors.Wrap(err, "read datacenters")
	}

	// Filter DCs
	if t.DC, err = dcfilter.Apply(dcMap, p.DC); err != nil {
		return t, err
	}

	// Ensure Host belongs to DCs
	if t.Host != "" && !hostBelongsToDCs(t.Host, t.DC, dcMap) {
		return t, service.ErrValidate(errors.Errorf("no such host %s in DC %s", t.Host, strings.Join(t.DC, ", ")))
	}

	// Filter keyspaces
	f, err := ksfilter.NewFilter(p.Keyspace)
	if err != nil {
		return t, err
	}

	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return t, errors.Wrapf(err, "read keyspaces")
	}

	dcs := strset.New(t.DC...)
	var skippedKeyspaces []string
	for _, keyspace := range keyspaces {
		tables, err := client.Tables(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: get tables", keyspace)
		}

		// Get the ring description and skip local data
		ring, err := client.DescribeRing(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: get ring description", keyspace)
		}

		// Ignore keyspaces not replicated in desired DCs
		if !dcs.HasAny(ring.Datacenters()...) {
			continue
		}

		if !singleNodeCluster(dcMap) {
			// Ignore not replicated keyspaces
			if ring.Replication == scyllaclient.LocalStrategy {
				continue
			}

			notEnoughReplicas := false
			for _, rt := range ring.ReplicaTokens {
				replicas := 0
				for _, r := range rt.ReplicaSet {
					if dcs.Has(ring.HostDC[r]) {
						replicas++
					}
				}
				if replicas <= 1 {
					notEnoughReplicas = true
					break
				}
			}
			if notEnoughReplicas {
				skippedKeyspaces = append(skippedKeyspaces, keyspace)
				continue
			}
		}

		// Add to the filter
		f.Add(keyspace, tables)
	}

	if len(skippedKeyspaces) > 0 {
		s.logger.Info(ctx,
			"Repair of the following keyspaces will be skipped because not all the tokens are present in the specified DCs",
			"keyspaces", strings.Join(skippedKeyspaces, ", "),
		)
	}

	// Get the filtered units
	t.Units, err = f.Apply(false)
	if err != nil {
		return t, errors.Wrap(ErrEmptyRepair, err.Error())
	}

	// Ignore nodes in status DOWN
	if p.IgnoreDownHosts {
		status, err := client.Status(ctx)
		if err != nil {
			return t, errors.Wrap(err, "status")
		}
		t.IgnoreHosts = status.Datacenter(t.DC).Down().Hosts()
	}

	return t, nil
}

func hostBelongsToDCs(host string, dcs []string, dcMap map[string][]string) bool {
	for _, dc := range dcs {
		for _, h := range dcMap[dc] {
			if host == h {
				return true
			}
		}
	}
	return false
}

func singleNodeCluster(dcMap map[string][]string) bool {
	if len(dcMap) == 1 {
		for _, dc := range dcMap {
			if len(dc) <= 1 {
				return true
			}
		}
	}
	return false
}

// Repair performs the repair process on the Target.
func (s *Service) Repair(ctx context.Context, clusterID, taskID, runID uuid.UUID, target Target) error {
	s.logger.Info(ctx, "Properties",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	run := &Run{
		ClusterID: clusterID,
		TaskID:    taskID,
		ID:        runID,
		DC:        target.DC,
		Host:      target.Host,
		StartTime: timeutc.Now(),
	}
	if err := s.putRun(run); err != nil {
		return errors.Wrapf(err, "put run")
	}

	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	if target.Continue {
		if err := s.fillPrevRunID(ctx, run); err != nil {
			return err
		}
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
		}
	}

	p, err := newPlan(ctx, target, client)
	if err != nil {
		return errors.Wrap(err, "create repair plan")
	}

	pm := &dbProgressManager{
		run:     run,
		session: s.session,
		metrics: s.metrics,
		logger:  s.logger.Named("progress"),
	}
	if err := pm.Init(p); err != nil {
		return err
	}
	pm.UpdatePlan(p)
	hosts := p.Hosts()

	ih, cleanup := s.newIntensityHandler(ctx, clusterID, target.Intensity, target.Parallel)
	defer cleanup()

	// In a multi-dc repair look for a local datacenter and assign host priorities
	hp := make(hostPriority)
	if len(target.DC) > 1 {
		dcMap, err := client.Datacenters(ctx)
		if err != nil {
			return errors.Wrap(err, "read datacenters")
		}

		targetDCs := strset.New(target.DC...)
		for dc := range dcMap {
			if !targetDCs.Has(dc) {
				delete(dcMap, dc)
			}
		}
		closest, err := client.ClosestDC(ctx, dcMap)
		if err != nil {
			return errors.Wrap(err, "calculate datacenter latency measurement")
		}

		for p, dc := range closest {
			for _, h := range dcMap[dc] {
				if slice.ContainsString(hosts, h) {
					hp[h] = p
				}
			}
		}
	}

	gen, err := newGenerator(ctx, target, p, client, ih, hp, s.logger)
	if err != nil {
		return errors.Wrap(err, "create generator")
	}

	if active, err := client.ActiveRepairs(ctx, hosts); err != nil {
		s.logger.Error(ctx, "Active repair check failed", "error", err)
	} else if len(active) > 0 {
		return errors.Errorf("ensure no active repair on hosts, %s are repairing", strings.Join(active, ", "))
	}

	// Both generator and workers use graceful ctx.
	//
	gracefulCtx, cancel := context.WithCancel(context.Background())
	gracefulCtx = log.CopyTraceID(gracefulCtx, ctx)
	done := make(chan struct{}, 1)

	var eg errgroup.Group
	for i := 0; i < gen.ctl.MaxParallel(); i++ {
		i := i
		eg.Go(func() error {
			w := &worker{
				in:         gen.next,
				out:        gen.result,
				client:     client,
				stopTrying: make(map[string]struct{}),
				progress:   pm,
				logger:     s.logger.Named(fmt.Sprintf("worker %d", i)),
			}
			w.Run(gracefulCtx)
			return nil
		})
	}
	eg.Go(func() error {
		return gen.Run(gracefulCtx)
	})

	go func() {
		select {
		case <-ctx.Done():
			s.logger.Info(ctx, "Start graceful stop period", "time", s.config.GracefulStopTimeout)
			gen.stopGenerating()
			time.AfterFunc(s.config.GracefulStopTimeout, cancel)
		case <-done:
			cancel()
		}
	}()

	if err = eg.Wait(); (err != nil && target.FailFast) || ctx.Err() != nil {
		s.killAllRepairs(ctx, client, hosts)
	}
	close(done)

	// Check if repair is fully finished
	if ok := gen.plan.UpdateIdx(); !ok {
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
	}

	// Prefer repair error to ctx error
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Service) killAllRepairs(ctx context.Context, client *scyllaclient.Client, hosts []string) {
	killCtx := log.CopyTraceID(context.Background(), ctx)
	killCtx = scyllaclient.Interactive(killCtx)
	if err := client.KillAllRepairs(killCtx, hosts...); err != nil {
		s.logger.Error(killCtx, "Failed to kill repairs", "hosts", hosts, "error", err)
	}
}

func hostMaxRanges(shards map[string]uint, memory map[string]int64) map[string]int {
	out := make(map[string]int, len(shards))
	for h, sh := range shards {
		out[h] = maxRepairRangesInParallel(sh, memory[h])
	}
	return out
}

func maxRepairRangesInParallel(shards uint, totalMemory int64) int {
	const MiB = 1024 * 1024
	memoryPerShard := totalMemory / int64(shards)
	max := int(0.1 * float64(memoryPerShard) / (32 * MiB) / 4)
	if max == 0 {
		max = 1
	}
	return max
}

func (s *Service) newIntensityHandler(ctx context.Context, clusterID uuid.UUID, intensity float64, parallel int) (ih *intensityHandler, cleanup func()) {
	ih = &intensityHandler{
		logger:    s.logger.Named("control"),
		intensity: atomic.NewFloat64(intensity),
		parallel:  atomic.NewInt64(int64(parallel)),
	}

	s.mu.Lock()
	if _, ok := s.intensityHandlers[clusterID]; ok {
		s.logger.Error(ctx, "Overriding intensity handler", "cluster_id", clusterID, "intensity", intensity, "parallel", parallel)
	}
	s.intensityHandlers[clusterID] = ih
	s.mu.Unlock()

	return ih, func() {
		s.mu.Lock()
		delete(s.intensityHandlers, clusterID)
		s.mu.Unlock()
	}
}

// putRun upserts a repair run.
func (s *Service) putRun(r *Run) error {
	return table.RepairRun.InsertQuery(s.session).BindStruct(r).ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(r); err != nil {
		s.logger.Error(ctx, "Cannot update the run",
			"run", r,
			"error", err,
		)
	}
}

// GetLastResumableRun returns the most recent started but not done run of
// the task, if there is a recent run that is completely done ErrNotFound is reported.
func (s *Service) GetLastResumableRun(clusterID, taskID uuid.UUID) (*Run, error) {
	q := qb.Select(table.RepairRun.Name()).Where(
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
		if s.isRunInitialized(r) {
			if !r.EndTime.IsZero() {
				break
			}
			return r, nil
		}
	}
	return nil, service.ErrNotFound
}

func (s *Service) isRunInitialized(run *Run) bool {
	q := qb.Select(table.RepairRunProgress.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
		qb.Eq("run_id"),
	).Limit(1).Query(s.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var check []*RunProgress
	err := q.SelectRelease(&check)
	return err == nil && len(check) > 0
}

func (s *Service) fillPrevRunID(ctx context.Context, run *Run) error {
	prev, err := s.GetLastResumableRun(run.ClusterID, run.TaskID)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			return nil
		}
		return errors.Wrap(err, "get previous run")
	}

	// Check if we can continue from prev
	s.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
	if s.config.AgeMax > 0 && timeutc.Since(prev.StartTime) > s.config.AgeMax {
		s.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	}

	run.PrevID = prev.ID
	return nil
}

// GetRun returns a run based on ID. If nothing was found scylla-manager.ErrNotFound
// is returned.
func (s *Service) GetRun(_ context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	var r Run
	return &r, table.RepairRun.GetQuery(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	}).GetRelease(&r)
}

// GetProgress returns run progress for all shards on all the hosts. If nothing
// was found scylla-manager.ErrNotFound is returned.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	run, err := s.GetRun(ctx, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, errors.Wrap(err, "get run")
	}
	manager := &dbProgressManager{
		run:     run,
		session: s.session,
	}

	p, err := manager.aggregateProgress()
	if err != nil {
		return Progress{}, errors.Wrap(err, "aggregate progress")
	}

	s.mu.Lock()
	if ih, ok := s.intensityHandlers[clusterID]; ok {
		p.Intensity = float64(ih.Intensity())
		p.Parallel = ih.Parallel()
	}
	s.mu.Unlock()

	p.DC = run.DC
	p.Host = run.Host
	return p, nil
}

// SetIntensity changes intensity of an ongoing repair.
func (s *Service) SetIntensity(ctx context.Context, clusterID uuid.UUID, intensity float64) error {
	s.mu.Lock()
	ih, ok := s.intensityHandlers[clusterID]
	s.mu.Unlock()

	if !ok {
		return errors.Wrap(service.ErrNotFound, "repair task")
	}

	if err := ih.SetIntensity(ctx, intensity); err != nil {
		return errors.Wrap(err, "set intensity")
	}

	return nil
}

// SetParallel changes parallelism of an ongoing repair.
func (s *Service) SetParallel(ctx context.Context, clusterID uuid.UUID, parallel int) error {
	s.mu.Lock()
	ih, ok := s.intensityHandlers[clusterID]
	s.mu.Unlock()

	if !ok {
		return errors.Wrap(service.ErrNotFound, "repair task")
	}

	if err := ih.SetParallel(ctx, parallel); err != nil {
		return errors.Wrap(err, "set parallel")
	}

	return nil
}

type intensityHandler struct {
	logger    log.Logger
	intensity *atomic.Float64
	parallel  *atomic.Int64
}

const (
	maxIntensity     = 0
	defaultIntensity = 1
	defaultParallel  = 0
)

// SetIntensity sets the value of '--intensity' flag.
func (i *intensityHandler) SetIntensity(ctx context.Context, intensity float64) error {
	if intensity < 0 {
		return service.ErrValidate(errors.Errorf("setting invalid intensity value %.2f", intensity))
	}

	i.logger.Info(ctx, "Setting repair intensity", "value", intensity, "previous", i.intensity.Load())
	i.intensity.Store(intensity)
	return nil
}

// SetParallel sets the value of '--parallel' flag.
func (i *intensityHandler) SetParallel(ctx context.Context, parallel int) error {
	if parallel < 0 {
		return service.ErrValidate(errors.Errorf("setting invalid parallel value %d", parallel))
	}

	i.logger.Info(ctx, "Setting repair parallel", "value", parallel, "previous", i.parallel.Load())
	i.parallel.Store(int64(parallel))
	return nil
}

// Intensity returns effective intensity.
func (i *intensityHandler) Intensity() int {
	return int(i.intensity.Load())
}

// Parallel returns stored value for parallel.
func (i *intensityHandler) Parallel() int {
	return int(i.parallel.Load())
}
