// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	"github.com/scylladb/scylla-manager/v3/pkg/util/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
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
		Intensity:           NewIntensityFromDeprecated(props.Intensity),
		Parallel:            props.Parallel,
		MaxJobsPerHost:      props.MaxJobsPerHost,
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
	if t.Parallel < 0 || t.Intensity < 0 || t.MaxJobsPerHost < 0 {
		return t, service.ErrValidate(errors.Errorf("flags --parallel (%v), --intensity (%v), --max-jobs-per-host (%v) cannot be negative", t.Parallel, t.Intensity, t.MaxJobsPerHost))
	}
	if t.MaxJobsPerHost > 1 && t.Parallel != 0 {
		return t, service.ErrValidate(errors.New("flag --max-jobs-per-host can be used only when --parallel is set to 0"))
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

	p, err := newPlan(ctx, t, client)
	if err != nil {
		return t, errors.Wrap(err, "create repair plan")
	}
	// Sort plan
	p.SizeSort()
	p.PrioritySort(NewInternalTablePreference())

	if clusterSession, err := s.clusterSession(ctx, clusterID); err != nil {
		s.logger.Info(ctx, "No cluster credentials, couldn't ensure repairing base table before its views", "error", err)
	} else {
		defer clusterSession.Close()
		views, err := query.GetAllViews(clusterSession)
		if err != nil {
			return t, errors.Wrap(err, "get cluster views")
		}
		p.ViewSort(views)
	}

	// Set filtered units as they are still used for displaying --dry-run
	t.Units = p.FilteredUnits(t.Units)
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
		Parallel:  target.Parallel,
		Intensity: target.Intensity,
		StartTime: timeutc.Now(),
	}
	if err := s.putRun(run); err != nil {
		return errors.Wrapf(err, "put run")
	}

	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	p, err := newPlan(ctx, target, client)
	if err != nil {
		return errors.Wrap(err, "create repair plan")
	}
	var ord int
	for _, kp := range p.Keyspaces {
		for _, tp := range kp.Tables {
			ord++
			s.logger.Info(ctx, "Repair order",
				"order", ord,
				"keyspace", kp.Keyspace,
				"table", tp.Table,
				"size", tp.Size,
				"merge_ranges", tp.Optimize,
			)
		}
	}

	pm := NewDBProgressManager(run, s.session, s.metrics, s.logger)
	prevID := uuid.Nil
	if target.Continue {
		if prev := pm.GetPrevRun(ctx, s.config.AgeMax); prev != nil {
			prevID = prev.ID
			// Respect parallel/intensity set in resumed run
			run.Parallel = prev.Parallel
			run.Intensity = prev.Intensity
		}
	}

	if err := pm.Init(p, prevID); err != nil {
		return err
	}
	s.putRunLogError(ctx, run)

	gracefulCtx, cancel := context.WithCancel(context.Background())
	gracefulCtx = log.CopyTraceID(gracefulCtx, ctx)
	defer cancel()

	// Create worker pool
	workers := workerpool.New[*worker, job, jobResult](gracefulCtx, func(ctx context.Context, i int) *worker {
		return &worker{
			client:     client,
			stopTrying: make(map[string]struct{}),
			progress:   pm,
			logger:     s.logger.Named(fmt.Sprintf("worker %d", i)),
		}
	}, chanSize)

	// Give intensity handler the ability to set pool size
	ih, cleanup := s.newIntensityHandler(ctx, clusterID, taskID, runID,
		p.MaxRepairRangesInParallel, p.MaxParallel, workers)
	defer cleanup()

	// Set controlled parameters
	ih.SetParallel(ctx, run.Parallel)
	ih.SetIntensity(ctx, run.Intensity)
	ih.SetMaxJobsPerHost(ctx, target.MaxJobsPerHost)

	// Give generator the ability to read parallel/intensity and
	// to submit and receive results from worker pool.
	gen, err := newGenerator(ctx, target, client, ih, workers, p, pm, s.logger)
	if err != nil {
		return errors.Wrap(err, "create generator")
	}

	done := make(chan struct{}, 1)
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

	if active, err := client.ActiveRepairs(ctx, p.Hosts); err != nil {
		s.logger.Error(ctx, "Active repair check failed", "error", err)
	} else if len(active) > 0 {
		return errors.Errorf("ensure no active repair on hosts, %s are repairing", strings.Join(active, ", "))
	}

	if err = gen.Run(gracefulCtx); (err != nil && target.FailFast) || ctx.Err() != nil {
		s.killAllRepairs(ctx, client, p.Hosts)
	}
	close(done)

	// Check if repair has ended successfully
	if err == nil && ctx.Err() == nil {
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
	}
	// Ensure that not interrupted repair has 100% progress (invalidate rounding errors).
	if ctx.Err() == nil && (!target.FailFast || err == nil) {
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

func (s *Service) newIntensityHandler(ctx context.Context, clusterID, taskID, runID uuid.UUID,
	maxHostIntensity map[string]int, maxParallel int, poolController sizeSetter,
) (ih *intensityHandler, cleanup func()) {
	ih = &intensityHandler{
		taskID:                    taskID,
		runID:                     runID,
		logger:                    s.logger.Named("control"),
		maxRepairRangesInParallel: maxHostIntensity,
		intensity:                 &atomic.Int64{},
		maxParallel:               maxParallel,
		parallel:                  &atomic.Int64{},
		maxJobsPerHost:            &atomic.Int64{},
		poolController:            poolController,
	}

	s.mu.Lock()
	if _, ok := s.intensityHandlers[clusterID]; ok {
		s.logger.Error(ctx, "Overriding intensity handler", "cluster_id", clusterID)
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
	p.Parallel = run.Parallel
	p.Intensity = run.Intensity

	// Set max parallel/intensity only for running tasks
	s.mu.Lock()
	if ih, ok := s.intensityHandlers[clusterID]; ok {
		maxI := NewIntensity(0)
		for _, v := range ih.MaxRepairRangesInParallel() {
			maxI = max(maxI, Intensity(v))
		}
		p.MaxIntensity = maxI
		p.MaxParallel = ih.MaxParallel()
	}
	s.mu.Unlock()

	p.DC = run.DC
	p.Host = run.Host
	return p, nil
}

// SetIntensity changes intensity of an ongoing repair.
func (s *Service) SetIntensity(ctx context.Context, clusterID uuid.UUID, intensity float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ih, ok := s.intensityHandlers[clusterID]
	if !ok {
		return errors.Wrap(service.ErrNotFound, "repair task")
	}
	if intensity < 0 {
		return service.ErrValidate(errors.Errorf("setting invalid intensity value %.2f", intensity))
	}
	ih.SetIntensity(ctx, NewIntensityFromDeprecated(intensity))

	err := table.RepairRun.UpdateBuilder("intensity").Query(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    ih.taskID,
		"id":         ih.runID,
		"intensity":  ih.Intensity(),
	}).ExecRelease()
	return errors.Wrap(err, "update db")
}

// SetParallel changes parallelism of an ongoing repair.
func (s *Service) SetParallel(ctx context.Context, clusterID uuid.UUID, parallel int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ih, ok := s.intensityHandlers[clusterID]
	if !ok {
		return errors.Wrap(service.ErrNotFound, "repair task")
	}
	if parallel < 0 {
		return service.ErrValidate(errors.Errorf("setting invalid parallel value %d", parallel))
	}
	if parallel != defaultParallel {
		if maxJobsPerHost := ih.maxJobsPerHost.Load(); maxJobsPerHost > 1 {
			return service.ErrValidate(errors.Errorf("when setting parallel to non zero value, please set max-jobs-per-host to one first"))
		}
	}
	ih.SetParallel(ctx, parallel)

	err := table.RepairRun.UpdateBuilder("parallel").Query(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    ih.taskID,
		"id":         ih.runID,
		"parallel":   ih.Parallel(),
	}).ExecRelease()
	return errors.Wrap(err, "update db")
}

// SetMaxJobsPerHost changes max-hosts-per-job of an ongoing repair.
func (s *Service) SetMaxJobsPerHost(ctx context.Context, clusterID uuid.UUID, maxJobsPerHost int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ih, ok := s.intensityHandlers[clusterID]
	if !ok {
		return errors.Wrap(service.ErrNotFound, "repair task")
	}
	if maxJobsPerHost < 0 {
		return service.ErrValidate(errors.Errorf("setting invalid max-jobs-per-host value %d", maxJobsPerHost))
	}
	if maxJobsPerHost > 1 {
		if parallel := ih.parallel.Load(); parallel != 0 {
			return service.ErrValidate(errors.Errorf("when setting max-jobs-per-host to a value greater than one, please set parallel to zero first"))
		}
	}

	ih.SetMaxJobsPerHost(ctx, maxJobsPerHost)
	return nil
}

type sizeSetter interface {
	SetSize(size int)
}

type intensityHandler struct {
	taskID                    uuid.UUID
	runID                     uuid.UUID
	logger                    log.Logger
	maxRepairRangesInParallel map[string]int
	intensity                 *atomic.Int64
	maxParallel               int
	parallel                  *atomic.Int64
	maxJobsPerHost            *atomic.Int64
	poolController            sizeSetter
}

const (
	maxIntensity     Intensity = 0
	defaultIntensity Intensity = 1
	defaultParallel            = 0
	chanSize                   = 10000
)

// SetIntensity sets the value of '--intensity' flag.
func (i *intensityHandler) SetIntensity(ctx context.Context, intensity Intensity) {
	i.logger.Info(ctx, "Setting repair intensity", "value", intensity, "previous", i.intensity.Load())
	i.intensity.Store(int64(intensity))
}

// SetParallel sets the value of '--parallel' flag.
func (i *intensityHandler) SetParallel(ctx context.Context, parallel int) {
	i.logger.Info(ctx, "Setting repair parallel", "value", parallel, "previous", i.parallel.Load())
	i.parallel.Store(int64(parallel))
	i.adjustPoolSize()
}

func (i *intensityHandler) SetMaxJobsPerHost(ctx context.Context, maxJobsPerHost int) {
	i.logger.Info(ctx, "Setting repair maxJobsPerHost", "value", maxJobsPerHost, "previous", i.maxJobsPerHost.Load())
	i.maxJobsPerHost.Store(int64(maxJobsPerHost))
	i.adjustPoolSize()
}

func (i *intensityHandler) adjustPoolSize() {
	parallel := i.parallel.Load()
	maxJobsPerHost := i.maxJobsPerHost.Load()
	var poolSize int
	if parallel == defaultParallel {
		poolSize = i.maxParallel * int(maxJobsPerHost)
	} else {
		poolSize = int(parallel)
	}
	i.poolController.SetSize(poolSize)
}

func (i *intensityHandler) ReplicaSetMinRepairRangesInParallel(replicaSet []string) int {
	out := math.MaxInt
	for _, rep := range replicaSet {
		out = min(i.maxRepairRangesInParallel[rep], out)
	}
	return out
}

// MaxRepairRangesInParallel returns max_token_ranges_in_parallel per host.
func (i *intensityHandler) MaxRepairRangesInParallel() map[string]int {
	return i.maxRepairRangesInParallel
}

// Intensity returns stored value for intensity.
func (i *intensityHandler) Intensity() Intensity {
	return NewIntensity(int(i.intensity.Load()))
}

// MaxParallel returns maximal achievable parallelism.
func (i *intensityHandler) MaxParallel() int {
	return i.maxParallel
}

// Parallel returns stored value for parallel.
func (i *intensityHandler) Parallel() int {
	return int(i.parallel.Load())
}

// MaxJobsPerHost returns stored value for maxJobsPerHost.
func (i *intensityHandler) MaxJobsPerHost() int {
	return int(i.maxJobsPerHost.Load())
}
