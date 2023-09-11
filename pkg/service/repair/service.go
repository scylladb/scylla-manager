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
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/dcfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
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
var ErrEmptyRepair = errors.New("no replicas to repair")

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (Target, error) {
	props := defaultTaskProperties()

	// Parse task properties
	if err := json.Unmarshal(properties, &props); err != nil {
		return Target{}, service.ErrValidate(errors.Wrapf(err, "parse runner properties: %s", properties))
	}

	// Copy basic properties
	t := Target{
		Host:                props.Host,
		FailFast:            props.FailFast,
		Continue:            props.Continue,
		Intensity:           props.Intensity,
		Parallel:            props.Parallel,
		SmallTableThreshold: props.SmallTableThreshold,
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrapf(err, "get client")
	}
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return t, errors.Wrap(err, "read datacenters")
	}
	status, err := client.Status(ctx)
	if err != nil {
		return t, errors.Wrap(err, "get status")
	}

	// Make it clear that repairing single node cluster does not make any sense (#1257)
	if len(status) < 1 {
		return t, service.ErrValidate(errors.New("repairing single node cluster does not have any effect"))
	}
	// Filter DCs
	if t.DC, err = dcfilter.Apply(dcMap, props.DC); err != nil {
		return t, err
	}
	// Ignore nodes with status DOWN
	if props.IgnoreDownHosts {
		t.IgnoreHosts = status.Datacenter(t.DC).Down().Hosts()
	}
	// Ensure Host is not ignored
	if t.Host != "" && slice.ContainsString(t.IgnoreHosts, t.Host) {
		return t, errors.New("host can't have status down")
	}
	// Ensure Host belongs to DCs
	if t.Host != "" && !hostBelongsToDCs(t.Host, t.DC, dcMap) {
		return t, service.ErrValidate(errors.Errorf("no such host %s in DC %s", t.Host, strings.Join(t.DC, ", ")))
	}

	// Get potential units - all tables matched by keyspace flag
	f, err := ksfilter.NewFilter(props.Keyspace)
	if err != nil {
		return t, err
	}
	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return t, errors.Wrapf(err, "get keyspaces")
	}
	for _, ks := range keyspaces {
		tables, err := client.Tables(ctx, ks)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: get tables", ks)
		}
		f.Add(ks, tables)
	}
	t.Units, err = f.Apply(false)
	if err != nil {
		return t, errors.Wrap(ErrEmptyRepair, err.Error())
	}

	t.plan, err = newPlan(ctx, t, client)
	if err != nil {
		return t, errors.Wrap(err, "create repair plan")
	}
	// Sort plan
	t.plan.SizeSort()
	t.plan.PrioritySort(NewInternalTablePreference())
	if clusterSession, err := s.clusterSession(ctx, clusterID); err != nil {
		s.logger.Info(ctx, "No cluster credentials, couldn't ensure repairing base table before its views", "error", err)
	} else {
		views, err := query.GetAllViews(clusterSession)
		if err != nil {
			return t, errors.Wrap(err, "get cluster views")
		}
		t.plan.ViewSort(views)
	}

	t.plan.SetMaxParallel(dcMap)
	if err := t.plan.SetMaxHostIntensity(ctx, client); err != nil {
		return t, errors.Wrap(err, "calculate intensity limits")
	}

	if len(t.plan.SkippedKeyspaces) > 0 {
		s.logger.Info(ctx,
			"Repair of the following keyspaces will be skipped because not all the tokens are present in the specified DCs",
			"keyspaces", strings.Join(t.plan.SkippedKeyspaces, ", "),
		)
	}

	// Set filtered units as they are still used for displaying --dry-run
	t.Units = t.plan.Units()
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

	pm := NewDBProgressManager(run, s.session, s.metrics, s.logger)
	if target.Continue {
		if err := pm.SetPrevRunID(ctx, s.config.AgeMax); err != nil {
			return errors.Wrap(err, "find previous run ID")
		}
	}
	if err := pm.Init(target.plan); err != nil {
		return err
	}
	pm.UpdatePlan(target.plan)
	s.putRunLogError(ctx, run)

	ih, cleanup := s.newIntensityHandler(ctx, clusterID,
		target.plan.MaxHostIntensity, target.Intensity, target.plan.MaxParallel, target.Parallel)
	defer cleanup()

	gen, err := newGenerator(ctx, target, client, ih, s.logger)
	if err != nil {
		return errors.Wrap(err, "create generator")
	}

	hosts := target.plan.Hosts()
	if active, err := client.ActiveRepairs(ctx, hosts); err != nil {
		s.logger.Error(ctx, "Active repair check failed", "error", err)
	} else if len(active) > 0 {
		return errors.Errorf("ensure no active repair on hosts, %s are repairing", strings.Join(active, ", "))
	}

	// Both generator and workers use graceful ctx.
	gracefulCtx, cancel := context.WithCancel(context.Background())
	gracefulCtx = log.CopyTraceID(gracefulCtx, ctx)
	done := make(chan struct{}, 1)

	var eg errgroup.Group
	for i := 0; i < ih.MaxParallel(); i++ {
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

	// Check if repair has ended successfully
	if err == nil && ctx.Err() == nil {
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
	}
	// Ensure that not interrupted repair has 100% progress (invalidate rounding errors).
	if gen.lastPercent == 100 {
		s.metrics.SetProgress(clusterID, 100)
	}

	return multierr.Append(err, ctx.Err())
}

func (s *Service) killAllRepairs(ctx context.Context, client *scyllaclient.Client, hosts []string) {
	killCtx := log.CopyTraceID(context.Background(), ctx)
	killCtx = scyllaclient.Interactive(killCtx)
	if err := client.KillAllRepairs(killCtx, hosts...); err != nil {
		s.logger.Error(killCtx, "Failed to kill repairs", "hosts", hosts, "error", err)
	}
}

func (s *Service) newIntensityHandler(ctx context.Context, clusterID uuid.UUID,
	maxHostIntensity map[string]int, intensity float64, maxParallel, parallel int,
) (ih *intensityHandler, cleanup func()) {
	ih = &intensityHandler{
		logger:           s.logger.Named("control"),
		maxHostIntensity: maxHostIntensity,
		intensity:        atomic.NewFloat64(intensity),
		maxParallel:      maxParallel,
		parallel:         atomic.NewInt64(int64(parallel)),
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

	pm := NewDBProgressManager(run, s.session, s.metrics, s.logger)
	p, err := pm.AggregateProgress()
	if err != nil {
		return Progress{}, errors.Wrap(err, "aggregate progress")
	}

	s.mu.Lock()
	if ih, ok := s.intensityHandlers[clusterID]; ok {
		max := 0
		for _, v := range ih.maxHostIntensity {
			if max < v {
				max = v
			}
		}

		p.MaxIntensity = float64(max)
		p.Intensity = float64(ih.Intensity())
		p.MaxParallel = ih.MaxParallel()
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
	logger           log.Logger
	maxHostIntensity map[string]int
	intensity        *atomic.Float64
	maxParallel      int
	parallel         *atomic.Int64
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

// MaxHostIntensity returns max_token_ranges_in_parallel per host.
func (i *intensityHandler) MaxHostIntensity() map[string]int {
	return i.maxHostIntensity
}

// Intensity returns effective intensity.
func (i *intensityHandler) Intensity() int {
	intensity := i.intensity.Load()
	// Deprecate float intensity
	if 0 < intensity && intensity < 1 {
		intensity = defaultIntensity
	}
	return int(intensity)
}

// MaxParallel returns maximal achievable parallelism.
func (i *intensityHandler) MaxParallel() int {
	return i.maxParallel
}

// Parallel returns stored value for parallel.
func (i *intensityHandler) Parallel() int {
	return int(i.parallel.Load())
}
