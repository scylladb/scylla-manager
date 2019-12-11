// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/util/httpmw"
	"github.com/scylladb/mermaid/pkg/util/inexlist/dcfilter"
	"github.com/scylladb/mermaid/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// Service orchestrates clusterName backups.
type Service struct {
	session *gocql.Session
	config  Config

	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	logger       log.Logger
}

func NewService(session *gocql.Session, config Config, clusterName ClusterNameFunc, scyllaClient scyllaclient.ProviderFunc, logger log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}

	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session:      session,
		config:       config,
		clusterName:  clusterName,
		scyllaClient: scyllaClient,
		logger:       logger,
	}, nil
}

// Runner creates a Runner that handles repairs.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}

// GetTarget converts runner properties into repair Target.
// It also ensures configuration for the backup providers is registered on the
// targeted hosts.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage, force bool) (Target, error) {
	p := defaultTaskProperties()
	t := Target{}

	if err := json.Unmarshal(properties, &p); err != nil {
		return t, service.ErrValidate(errors.Wrapf(err, "parse runner properties: %s", properties))
	}

	if p.Location == nil {
		return t, errors.Errorf("missing location")
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

	// Validate location DCs
	if err := checkDCs(func(i int) (string, string) { return p.Location[i].DC, p.Location[i].String() }, len(p.Location), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid location")
	}

	// Validate rate limit DCs
	if err := checkDCs(dcLimitDCAtPos(p.RateLimit), len(p.RateLimit), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid rate-limit")
	}

	// Validate upload parallel DCs
	if err := checkDCs(dcLimitDCAtPos(p.SnapshotParallel), len(p.SnapshotParallel), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid snapshot-parallel")
	}

	// Validate snapshot parallel DCs
	if err := checkDCs(dcLimitDCAtPos(p.UploadParallel), len(p.UploadParallel), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid upload-parallel")
	}

	// Copy simple properties
	t.Retention = p.Retention
	t.Continue = p.Continue

	// Filter DCs
	if t.DC, err = dcfilter.Apply(dcMap, p.DC); err != nil {
		return t, err
	}

	// Filter out properties of not used DCs
	t.Location = filterDCLocations(p.Location, t.DC)
	t.RateLimit = filterDCLimits(p.RateLimit, t.DC)
	t.SnapshotParallel = filterDCLimits(p.SnapshotParallel, t.DC)
	t.UploadParallel = filterDCLimits(p.UploadParallel, t.DC)

	if err := checkAllDCsCovered(t.Location, t.DC); err != nil {
		return t, errors.Wrap(err, "invalid location")
	}

	// Validate that locations are accessible from the nodes
	if err := s.checkLocationsAvailableFromDCs(ctx, client, t.Location, t.DC, dcMap); err != nil {
		return t, errors.Wrap(err, "location is not accessible")
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
		if ring.Replication == scyllaclient.LocalStrategy {
			if strings.HasPrefix(keyspace, "system") && keyspace != "system_schema" {
				continue
			}
		}

		// Add to the filter
		f.Add(keyspace, tables)
	}

	// Get the filtered units
	v, err := f.Apply(force)
	if err != nil {
		return t, err
	}

	// Copy units
	for _, u := range v {
		uu := Unit{
			Keyspace:  u.Keyspace,
			Tables:    u.Tables,
			AllTables: u.AllTables,
		}
		t.Units = append(t.Units, uu)
	}

	return t, nil
}

// GetTargetSize calculates total size of the backup for the provided target.
func (s *Service) GetTargetSize(ctx context.Context, clusterID uuid.UUID, target Target) (int64, error) {
	s.logger.Info(ctx, "Calculating target size")

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client")
	}
	// Get hosts in all DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "read datacenters")
	}
	// Get hosts in the given DCs
	hosts := dcHosts(dcMap, target.DC)
	if len(hosts) == 0 {
		return 0, errors.New("no matching hosts found")
	}

	// Create index of all call parameters
	type hut struct {
		Host  int
		Unit  int
		Table int
	}
	var idx []hut
	for u, v := range target.Units {
		for t := range v.Tables {
			// Put hosts last to distribute load on hosts evenly
			for h := range hosts {
				idx = append(idx, hut{h, u, t})
			}
		}
	}

	// Get shard count of a first node to estimate parallelism limit
	shards, err := client.ShardCount(ctx, hosts[0])
	if err != nil {
		return 0, errors.Wrapf(err, "%s: shard count", hosts[0])
	}

	var (
		limit = len(hosts) * int(shards)
		total atomic.Int64
	)
	err = parallel.Run(len(idx), limit, func(i int) error {
		v := idx[i]
		h := hosts[v.Host]
		k := target.Units[v.Unit].Keyspace
		t := target.Units[v.Unit].Tables[v.Table]

		size, err := client.TableDiskSize(ctx, h, k, t)
		if err != nil {
			return parallel.Abort(errors.Wrapf(err, h))
		}
		s.logger.Debug(ctx, "Table disk size",
			"host", h,
			"keyspace", k,
			"table", t,
			"size", size,
		)
		total.Add(size)

		return nil
	})

	return total.Load(), err
}

// checkLocationsAvailableFromDCs checks if each node in every DC has access to
// location assigned to that DC.
func (s *Service) checkLocationsAvailableFromDCs(ctx context.Context, client *scyllaclient.Client, locations []Location, dcs []string, dcMap map[string][]string) error {
	s.logger.Info(ctx, "Checking accessibility of remote locations")
	defer s.logger.Info(ctx, "Done checking accessibility of remote locations")

	// Get hosts of the target DCs
	hosts := dcHosts(dcMap, dcs)
	if len(hosts) == 0 {
		return errors.New("no matching hosts found")
	}

	var (
		errs error
		res  = make(chan error)
	)

	for _, l := range locations {
		if l.DC != "" && !sliceContains(l.DC, dcs) {
			continue
		}
		lh := hosts
		if l.DC != "" {
			lh = dcHosts(dcMap, []string{l.DC})
			if len(lh) == 0 {
				s.logger.Error(ctx, "No matching hosts found", "location", l)
				continue
			}
		}

		for _, h := range lh {
			go func(h string, l Location) {
				res <- s.checkHostLocation(ctx, client, h, l)
			}(h, l)
		}

		for range lh {
			errs = multierr.Append(errs, <-res)
		}
	}

	return service.ErrValidate(errs)
}

func (s *Service) checkHostLocation(ctx context.Context, client *scyllaclient.Client, h string, l Location) error {
	_, err := client.RcloneListDir(httpmw.DontRetry(ctx), h, l.RemotePath(""), nil)
	if err != nil {
		s.logger.Info(ctx, "Host location check FAILED", "host", h, "location", l, "error", err)
		var e error
		if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
			e = errors.Errorf("%s: failed to access %s make sure that the location is correct and credentials are set", h, l)
		} else {
			e = errors.Wrapf(err, "%s: access %s", h, l)
		}
		return e
	}
	s.logger.Info(ctx, "Host location check OK", "host", h, "location", l)
	return nil
}

// ExtractLocations parses task properties and returns list of locations.
// Each location is returned once. Same locations with different DCs are
// assumed equal.
func (s *Service) ExtractLocations(ctx context.Context, properties []json.RawMessage) []Location {
	l, err := extractLocations(properties)
	if err != nil {
		s.logger.Debug(ctx, "Failed to extract some locations", "error", err)
	}
	return l
}

// List returns available snapshots in remote locations.
func (s *Service) List(ctx context.Context, clusterID uuid.UUID, locations []Location, filter ListFilter) ([]ListItem, error) {
	s.logger.Info(ctx, "Listing backups",
		"cluster_id", clusterID,
		"locations", locations,
		"filter", filter,
	)

	manifests, err := s.list(ctx, clusterID, locations, filter, false)
	if err != nil {
		return nil, err
	}

	return aggregateRemoteManifests(manifests), nil
}

// ListFiles returns info on available backup files based on filtering criteria.
func (s *Service) ListFiles(ctx context.Context, clusterID uuid.UUID, locations []Location, filter ListFilter) ([]FilesInfo, error) {
	s.logger.Info(ctx, "Listing backup files",
		"cluster_id", clusterID,
		"locations", locations,
		"filter", filter,
	)

	manifests, err := s.list(ctx, clusterID, locations, filter, true)
	if err != nil {
		return nil, err
	}

	var files = make([]FilesInfo, len(manifests))
	for i := range manifests {
		files[i] = makeFilesInfo(manifests[i])
	}
	return files, nil
}

func (s *Service) list(ctx context.Context, clusterID uuid.UUID, locations []Location, filter ListFilter, load bool) ([]remoteManifest, error) {
	// Validate inputs
	if len(locations) == 0 {
		return nil, service.ErrValidate(errors.New("empty locations"))
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get client proxy")
	}

	// Resolve hosts for locations
	hosts := make([]hostInfo, len(locations))
	for i := range locations {
		hosts[i].Location = locations[i]
	}
	if err := s.resolveHosts(ctx, client, hosts); err != nil {
		return nil, errors.Wrap(err, "resolve hosts")
	}

	// List manifests
	type manifestsError struct {
		Manifests []remoteManifest
		Err       error
	}
	res := make(chan manifestsError)
	for _, item := range hosts {
		go func(h string, l Location) {
			s.logger.Info(ctx, "Listing remote manifests",
				"cluster_id", clusterID,
				"host", h,
				"location", l,
			)
			m, err := listManifests(ctx, client, h, l, filter, load, s.logger.Named("list"))
			res <- manifestsError{m, errors.Wrapf(err, "%s: list remote files at location %s", h, l)}
		}(item.IP, item.Location)
	}

	var (
		manifests []remoteManifest
		errs      error
	)
	for range hosts {
		r := <-res
		manifests = append(manifests, r.Manifests...)
		errs = multierr.Append(errs, r.Err)
	}
	return manifests, errs
}

func (s *Service) resolveHosts(ctx context.Context, client *scyllaclient.Client, hosts []hostInfo) error {
	s.logger.Debug(ctx, "Resolving hosts for locations")

	var (
		dcMap map[string][]string
		err   error
	)

	// Check if we need to load DC map
	hasDC := false
	for i := range hosts {
		if hosts[i].Location.DC != "" {
			hasDC = true
			break
		}
	}
	// Load DC map if needed
	if hasDC {
		dcMap, err = client.Datacenters(ctx)
		if err != nil {
			return errors.Wrap(err, "read datacenters")
		}
	}

	// Config hosts has nice property that hosts are sorted by closest DC
	allHosts := client.Config().Hosts

	return parallel.Run(len(hosts), parallel.NoLimit, func(i int) error {
		l := hosts[i].Location

		checklist := allHosts
		if l.DC != "" {
			checklist = dcMap[l.DC]
		}

		if len(checklist) == 0 {
			return errors.Errorf("no matching hosts found for location %s", l)
		}

		for _, h := range checklist {
			_, err := client.RcloneListDir(httpmw.DontRetry(ctx), h, l.RemotePath(""), nil)
			if err != nil {
				s.logger.Debug(ctx, "Host location check FAILED", "host", h, "location", l, "error", err)
			} else {
				s.logger.Debug(ctx, "Host location check OK", "host", h, "location", l)

				hosts[i].IP = h
				return nil
			}
		}

		return errors.Errorf("no matching hosts found for location %s", l)
	})
}

// Backup executes a backup on a given target.
func (s *Service) Backup(ctx context.Context, clusterID, taskID, runID uuid.UUID, target Target) error {
	s.logger.Debug(ctx, "Backup",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	run := &Run{
		ClusterID: clusterID,
		TaskID:    taskID,
		ID:        runID,
		Units:     target.Units,
		DC:        target.DC,
		Location:  target.Location,
		StartTime: timeutc.Now().UTC(),
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}
	run.clusterName = clusterName

	s.logger.Info(ctx, "Initializing backup",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
		"target", target,
	)

	// Get the cluster client
	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "get client proxy")
	}

	// Get hosts in all DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return errors.Wrap(err, "read datacenters")
	}

	// Get hosts in the given DCs
	hosts := dcHosts(dcMap, run.DC)
	if len(hosts) == 0 {
		return errors.New("no matching hosts found")
	}

	// Get host IDs
	hostIDs, err := client.HostIDs(ctx)
	if err != nil {
		return errors.Wrap(err, "get host IDs")
	}

	// Create hostInfo for hosts
	hi, err := hostInfoFromHosts(hosts, dcMap, hostIDs, target.Location, target.RateLimit)
	if err != nil {
		return err
	}

	if target.Continue {
		if err := s.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
		}
	}

	// Generate snapshot tag
	if run.SnapshotTag == "" {
		run.SnapshotTag = newSnapshotTag()
	}

	// Register the run
	if err := s.putRun(run); err != nil {
		return errors.Wrap(err, "register the run")
	}

	// Create a worker
	w := &worker{
		ClusterID:     clusterID,
		TaskID:        taskID,
		RunID:         runID,
		SnapshotTag:   run.SnapshotTag,
		Config:        s.config,
		Units:         run.Units,
		Client:        client,
		OnRunProgress: s.putRunProgressLogError,
	}

	// Start metric updater
	stopMetricsUpdater := newBackupMetricUpdater(ctx, run, NewProgressVisitor(run, s.session), s.logger.Named("metrics"), service.PrometheusScrapeInterval)
	defer stopMetricsUpdater()

	// Take snapshot if needed
	if run.PrevID == uuid.Nil {
		w = w.WithLogger(s.logger.Named("snapshot"))
		if err := w.Snapshot(ctx, hi, target.SnapshotParallel); err != nil {
			return errors.Wrap(err, "snapshot")
		}
	}

	// Upload files
	w = w.WithLogger(s.logger.Named("upload"))
	if err := w.Upload(ctx, hi, target.UploadParallel); err != nil {
		return errors.Wrap(err, "upload")
	}

	// Move manifests
	w = w.WithLogger(s.logger.Named("move"))
	if err := w.MoveManifests(ctx, hi); err != nil {
		return errors.Wrap(err, "move manifests")
	}

	// Purge remote data
	w = w.WithLogger(s.logger.Named("purge"))
	if err := w.Purge(ctx, hi, target.Retention); err != nil {
		return errors.Wrap(err, "purge")
	}

	return errors.Wrap(s.markRunAsDone(run), "mark run as done")
}

// decorateWithPrevRun gets task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRun(ctx context.Context, run *Run) error {
	prev, err := s.GetLastResumableRun(ctx, run.ClusterID, run.TaskID)
	if err == service.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get previous run")
	}

	// Check if can continue from prev
	s.logger.Info(ctx, "Found previous run", "run_id", prev.ID)
	if timeutc.Since(prev.StartTime) > s.config.AgeMax {
		s.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	}

	run.PrevID = prev.ID
	run.SnapshotTag = prev.SnapshotTag
	run.Units = prev.Units
	run.DC = prev.DC

	return nil
}

// GetLastResumableRun returns the the most recent started but not done run of
// the task, if there is a recent run that is completely done ErrNotFound is
// reported.
func (s *Service) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	stmt, names := qb.Select(table.BackupRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(20).ToCql()
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*Run
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	hasProgress := s.hasProgressQuery()
	defer hasProgress.Release()

	var rp RunProgress
	for _, r := range runs {
		if r.Done {
			break
		}

		err := hasProgress.BindMap(qb.M{
			"cluster_id": r.ClusterID,
			"task_id":    r.TaskID,
			"run_id":     r.ID,
		}).Get(&rp)

		if err == gocql.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	return nil, service.ErrNotFound
}

func (s *Service) hasProgressQuery() *gocqlx.Queryx {
	stmt, names := qb.Select(table.BackupRunProgress.Name()).
		Columns("run_id").
		Where(
			qb.Eq("cluster_id"),
			qb.Eq("task_id"),
			qb.Eq("run_id"),
		).
		Limit(1).
		ToCql()
	return gocqlx.Query(s.session.Query(stmt), names)
}

// putRun upserts a backup run.
func (s *Service) putRun(r *Run) error {
	stmt, names := table.BackupRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(r); err != nil {
		s.logger.Error(ctx, "Failed to update the run",
			"run", r,
			"error", err,
		)
	}
}

// markRunAsDone updates and persists done value.
func (s *Service) markRunAsDone(run *Run) error {
	run.Done = true

	stmt, names := table.BackupRun.Update("done")
	return gocqlx.Query(s.session.Query(stmt), names).BindStruct(run).ExecRelease()
}

// putRunProgress upserts a backup run progress.
func (s *Service) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	stmt, names := table.BackupRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(p)

	return q.ExecRelease()
}

// putRunProgressLogError executes putRunProgress and consumes the error.
func (s *Service) putRunProgressLogError(ctx context.Context, p *RunProgress) {
	if err := s.putRunProgress(ctx, p); err != nil {
		s.logger.Error(ctx, "Failed to update file progress",
			"progress", p,
			"error", err,
		)
	}
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	stmt, names := table.BackupRun.Get()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r Run
	return &r, q.GetRelease(&r)
}

// GetProgress aggregates progress for the run of the task and breaks it down
// by keyspace and table.json
// If nothing was found mermaid.ErrNotFound is returned.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	s.logger.Debug(ctx, "GetProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	run, err := s.GetRun(ctx, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, err
	}

	return aggregateProgress(run, NewProgressVisitor(run, s.session))
}
