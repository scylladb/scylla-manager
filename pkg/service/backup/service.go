// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/dcfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/jsonutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

const defaultRateLimit = 100 // 100MiB

// Service orchestrates clusterName backups.
type Service struct {
	session gocqlx.Session
	config  Config
	metrics metrics.BackupMetrics

	clusterName    cluster.NameFunc
	scyllaClient   scyllaclient.ProviderFunc
	clusterSession cluster.SessionFunc
	configCache    configcache.ConfigCacher
	logger         log.Logger

	dth deduplicateTestHooks
}

func NewService(session gocqlx.Session, config Config, metrics metrics.BackupMetrics, clusterName cluster.NameFunc,
	scyllaClient scyllaclient.ProviderFunc, clusterSession cluster.SessionFunc, configCache configcache.ConfigCacher,
	logger log.Logger,
) (*Service, error) {
	if session.Session == nil || session.Closed() {
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
		session:        session,
		config:         config,
		metrics:        metrics,
		clusterName:    clusterName,
		scyllaClient:   scyllaClient,
		clusterSession: clusterSession,
		configCache:    configCache,
		logger:         logger,
	}, nil
}

// Runner creates a Runner that handles repairs.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}

// TaskDecorator generates "retention_map" for backup task.
func (s *Service) TaskDecorator(schedSvc *scheduler.Service) func(ctx context.Context, clusterID, taskID uuid.UUID, properties json.RawMessage) (json.RawMessage, error) {
	return func(ctx context.Context, clusterID, _ uuid.UUID, properties json.RawMessage) (json.RawMessage, error) {
		tasks, err := schedSvc.ListTasks(ctx, clusterID, scheduler.ListFilter{TaskType: []scheduler.TaskType{scheduler.BackupTask}})
		if err != nil {
			return nil, err
		}
		retentionMap := make(RetentionMap)
		for _, t := range tasks {
			r, err := ExtractRetention(t.Properties)
			if err != nil {
				return nil, errors.Wrapf(err, "extract retention for task %s", t.ID)
			}
			retentionMap[t.ID] = r
		}
		return jsonutil.Set(properties, "retention_map", retentionMap), nil
	}
}

// GetRetention returns the retention policy for a given task ID.
// In case task ID cannot be found in the RetentionMap, default retention for deleted task is returned.
func GetRetention(taskID uuid.UUID, retentionMap RetentionMap) RetentionPolicy {
	if r, ok := retentionMap[taskID]; ok {
		return r
	}
	return defaultRetentionForDeletedTask()
}

// GetTarget converts runner properties into backup Target.
// It also ensures configuration for the backup providers is registered on the targeted hosts.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (Target, error) {
	return s.targetFromProperties(ctx, clusterID, properties)
}

func (s *Service) targetFromProperties(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (Target, error) {
	s.logger.Info(ctx, "Generating backup target", "cluster_id", clusterID)

	p := defaultTaskProperties()
	if err := json.Unmarshal(properties, &p); err != nil {
		return Target{}, util.ErrValidate(err)
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return Target{}, errors.Wrapf(err, "get client")
	}

	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return Target{}, errors.Wrap(err, "read datacenters")
	}
	dcs, err := dcfilter.Apply(dcMap, p.DC)
	if err != nil {
		return Target{}, err
	}

	if err := p.validate(dcs, dcMap); err != nil {
		return Target{}, err
	}

	liveNodes, err := s.getLiveNodes(ctx, client, dcs)
	if err != nil {
		return Target{}, err
	}
	if err := s.checkLocationsAvailableFromNodes(ctx, client, liveNodes, p.Location); err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return Target{}, errors.New("specified bucket does not exist")
		}
		return Target{}, errors.Wrap(err, "location is not accessible")
	}

	f, err := ksfilter.NewFilter(p.Keyspace)
	if err != nil {
		return Target{}, err
	}

	ks, err := client.KeyspacesByType(ctx)
	if err != nil {
		return Target{}, errors.Wrap(err, "get keyspaces by type")
	}

	filters := []tabFilter{
		patternFilter{pattern: f},
		dcFilter{dcs: strset.New(dcs...)},
		localDataFilter{keyspaces: ks},
	}

	// Try to add view filter - possible only when credentials are set
	session, err := s.clusterSession(ctx, clusterID)
	switch {
	case err == nil:
		defer session.Close()
		views, err := query.GetAllViews(session)
		if err != nil {
			return Target{}, errors.Wrap(err, "get cluster views")
		}
		filters = append(filters, viewFilter{views: views})
	case errors.Is(err, cluster.ErrNoCQLCredentials):
		s.logger.Error(ctx, "No CQL cluster credentials, backup of views won't be skipped", "error", err)
	default:
		return Target{}, errors.Wrap(err, "create cluster session")
	}

	liveNodeIPs, err := slices.MapWithError(liveNodes.Hosts(), netip.ParseAddr)
	if err != nil {
		return Target{}, err
	}
	liveNodesSet := maps.SetFromSlice(liveNodeIPs)
	validators := []tabValidator{
		tokenRangesValidator{
			liveNodes: &liveNodesSet,
			dcs:       strset.New(dcs...),
		},
	}

	return p.toTarget(ctx, client, dcs, liveNodes, filters, validators)
}

// getLiveNodes returns live nodes of specified datacenters.
func (s *Service) getLiveNodes(ctx context.Context, client *scyllaclient.Client, dcs []string) (scyllaclient.NodeStatusInfoSlice, error) {
	// Get hosts in all DCs
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get result")
	}

	// Filter live nodes
	nodes := status.Datacenter(dcs)
	liveNodes, err := client.GetLiveNodes(ctx, status.Datacenter(dcs))
	if err != nil {
		return nil, err
	}

	if len(liveNodes) < len(nodes) {
		dead := strset.New(nodes.Hosts()...)
		dead.Remove(liveNodes.Hosts()...)
		s.logger.Info(ctx, "Ignoring down nodes", "hosts", dead)
	}

	return liveNodes, nil
}

// checkLocationsAvailableFromNodes checks if each node has access location for its datacenter.
func (s *Service) checkLocationsAvailableFromNodes(ctx context.Context, client *scyllaclient.Client,
	nodes scyllaclient.NodeStatusInfoSlice, locations []backupspec.Location,
) error {
	s.logger.Info(ctx, "Checking accessibility of remote locations")
	defer s.logger.Info(ctx, "Done checking accessibility of remote locations")

	// DC location index
	dcl := map[string]backupspec.Location{}
	for _, l := range locations {
		dcl[l.DC] = l
	}

	f := func(i int) error {
		n := nodes[i]
		l, ok := dcl[n.Datacenter]
		if !ok {
			l = dcl[""]
		}
		return s.checkHostLocation(ctx, client, n.Addr, l)
	}

	notify := func(i int, err error) {
		n := nodes[i]
		l, ok := dcl[n.Datacenter]
		if !ok {
			l = dcl[""]
		}
		s.logger.Error(ctx, "Failed to access location from node",
			"node", n.Addr,
			"location", l,
			"error", err,
		)
	}

	// Run checkHostLocation in parallel
	return util.ErrValidate(parallel.Run(len(nodes), parallel.NoLimit, f, notify))
}

func (s *Service) checkHostLocation(ctx context.Context, client *scyllaclient.Client, h string, l backupspec.Location) error {
	err := client.RcloneCheckPermissions(ctx, h, l.RemotePath(""))
	if err != nil {
		s.logger.Info(ctx, "Location check FAILED", "host", h, "location", l, "error", err)

		if scyllaclient.StatusCodeOf(err) > 0 {
			tip := fmt.Sprintf("make sure the location is correct and credentials are set, to debug SSH to %s and run \"scylla-manager-agent check-location -L %s --debug\"", h, l)
			err = fmt.Errorf("%s: %w - %s", h, err, tip)
		} else {
			err = fmt.Errorf("%s: %w", h, err)
		}
		return err
	}

	s.logger.Info(ctx, "Location check OK", "host", h, "location", l)
	return nil
}

// GetTargetSize calculates total size of the backup for the provided target.
func (s *Service) GetTargetSize(ctx context.Context, clusterID uuid.UUID, target Target) (int64, error) {
	s.logger.Info(ctx, "Calculating backup size")

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client")
	}

	// Get hosts in the given DCs
	hosts := target.liveNodes.Datacenter(target.DC).Hosts()

	var idx []scyllaclient.HostKeyspaceTable
	for _, v := range target.Units {
		for _, t := range v.Tables {
			// Put hosts last to distribute load on hosts evenly
			for _, h := range hosts {
				idx = append(idx, scyllaclient.HostKeyspaceTable{Host: h, Keyspace: v.Keyspace, Table: t})
			}
		}
	}

	report, err := client.TableDiskSizeReport(ctx, idx)
	if err != nil {
		return 0, errors.Wrap(err, "table disk size report")
	}

	var total int64
	for _, sr := range report {
		total += sr.Size
	}

	return total, err
}

// ExtractLocations parses task properties and returns list of locations.
// Each location is returned once. Same locations with different DCs are
// assumed equal.
func (s *Service) ExtractLocations(ctx context.Context, properties []json.RawMessage) []backupspec.Location {
	l, err := extractLocations(properties)
	if err != nil {
		s.logger.Debug(ctx, "Failed to extract some locations", "error", err)
	}
	return l
}

// List returns available snapshots in remote locations.
func (s *Service) List(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, filter ListFilter) ([]ListItem, error) {
	s.logger.Info(ctx, "Listing backups",
		"cluster_id", clusterID,
		"locations", locations,
		"filter", filter,
	)

	var items []ListItem

	handler := func(mc backupspec.ManifestInfoWithContent) error {
		// Calculate size on filtered units
		var (
			size    int64
			visited bool
		)
		if err := mc.ForEachIndexIter(filter.Keyspace, func(u backupspec.FilesMeta) {
			size += u.Size
			visited = true
		}); err != nil {
			return err
		}
		// Skip manifest if it does not contain any interesting data
		if !visited {
			return nil
		}

		// Find list item or create a new one
		var ptr *ListItem
		for i, li := range items {
			if li.ClusterID == mc.ClusterID && li.TaskID == mc.TaskID {
				ptr = &items[i]
				break
			}
		}
		if ptr == nil {
			items = append(items, ListItem{
				ClusterID: mc.ClusterID,
				TaskID:    mc.TaskID,
				unitCache: make(map[string]*strset.Set),
			})
			ptr = &items[len(items)-1]
		}

		// Find snapshot info or create a new one
		var siptr *SnapshotInfo
		for i, r := range ptr.SnapshotInfo {
			if r.SnapshotTag == mc.SnapshotTag {
				siptr = &ptr.SnapshotInfo[i]
				break
			}
		}
		if siptr == nil {
			ptr.SnapshotInfo = append(ptr.SnapshotInfo, SnapshotInfo{
				SnapshotTag: mc.SnapshotTag,
				Nodes:       1,
				Size:        size,
			})
		} else {
			siptr.Nodes++
			siptr.Size += size
		}

		// Add unit information from index
		return mc.ForEachIndexIter(filter.Keyspace, func(u backupspec.FilesMeta) {
			s, ok := ptr.unitCache[u.Keyspace]
			if !ok {
				ptr.unitCache[u.Keyspace] = strset.New(u.Table)
			} else {
				s.Add(u.Table)
			}
		})
	}
	if err := s.forEachManifest(ctx, clusterID, locations, filter, handler); err != nil {
		return nil, err
	}

	// Post processing...
	for k := range items {
		ptr := &items[k]

		// Sort Snapshots in descending order
		sort.Slice(ptr.SnapshotInfo, func(i, j int) bool {
			return ptr.SnapshotInfo[i].SnapshotTag > ptr.SnapshotInfo[j].SnapshotTag
		})
		// Create Units from cache
		for k, s := range ptr.unitCache {
			u := Unit{
				Keyspace: k,
				Tables:   s.List(),
			}
			sort.Strings(u.Tables)
			ptr.Units = append(ptr.Units, u)
		}
	}

	return items, nil
}

// ListFiles returns info on available backup files based on filtering criteria.
func (s *Service) ListFiles(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, filter ListFilter) ([]backupspec.FilesInfo, error) {
	s.logger.Info(ctx, "Listing backup files",
		"cluster_id", clusterID,
		"locations", locations,
		"filter", filter,
	)

	var files []backupspec.FilesInfo

	handler := func(mc backupspec.ManifestInfoWithContent) error {
		l := mc.Location
		l.DC = ""

		fi := backupspec.FilesInfo{
			Location: l,
			Schema:   mc.Schema,
		}

		if err := mc.ForEachIndexIter(filter.Keyspace, func(u backupspec.FilesMeta) {
			u.Path = mc.SSTableVersionDir(u.Keyspace, u.Table, u.Version)
			fi.Files = append(fi.Files, u)
		}); err != nil {
			return err
		}
		// Skip manifest if it does not contain any interesting data
		if fi.Files != nil {
			files = append(files, fi)
		}
		return nil
	}

	return files, s.forEachManifest(ctx, clusterID, locations, filter, handler)
}

// forEachManifest fetches and performs callback on manifests from remote locations.
//
// NOTE: It does not load Index into memory. In case callback requires access to Index,
// it should use ForEachIndexIter or ForEachIndexIterFiles.
func (s *Service) forEachManifest(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location,
	filter ListFilter, f func(backupspec.ManifestInfoWithContent) error,
) error {
	// Validate inputs
	if len(locations) == 0 {
		return util.ErrValidate(errors.New("empty locations"))
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get client proxy")
	}

	// Resolve hosts for locations
	hosts := make([]hostInfo, len(locations))
	for i := range locations {
		hosts[i].Location = locations[i]
	}
	if err := s.resolveHosts(ctx, client, hosts); err != nil {
		return errors.Wrap(err, "resolve hosts")
	}
	locationHost := map[backupspec.Location]string{}
	for i := range hosts {
		locationHost[hosts[i].Location] = hosts[i].IP
	}

	manifests, err := listManifestsInAllLocations(ctx, client, hosts, filter.ClusterID)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	manifests = filterManifests(manifests, filter)

	// Load manifest content
	load := func(c *backupspec.ManifestContentWithIndex, m *backupspec.ManifestInfo) error {
		r, err := client.RcloneOpen(ctx, locationHost[m.Location], m.Location.RemotePath(m.Path()))
		if err != nil {
			return err
		}
		defer r.Close()

		return c.Read(r)
	}

	for _, m := range manifests {
		c := new(backupspec.ManifestContentWithIndex)
		if err := load(c, m); err != nil {
			return err
		}

		if err := f(backupspec.ManifestInfoWithContent{
			ManifestInfo:             m,
			ManifestContentWithIndex: c,
		}); err != nil {
			return err
		}
	}
	return nil
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

	f := func(i int) error {
		l := hosts[i].Location

		checklist := allHosts
		if l.DC != "" {
			checklist = dcMap[l.DC]
		}

		if len(checklist) == 0 {
			return errors.Errorf("no matching hosts found for location %s", l)
		}

		for _, h := range checklist {
			_, err := client.RcloneListDir(ctx, h, l.RemotePath(""), nil)
			if err != nil {
				s.logger.Debug(ctx, "Location check FAILED", "host", h, "location", l, "error", err)
			} else {
				s.logger.Debug(ctx, "Location check OK", "host", h, "location", l)

				hosts[i].IP = h
				return nil
			}
		}

		return errors.Errorf("no matching hosts found for location %s", l)
	}

	notify := func(i int, err error) {
		h := hosts[i]
		s.logger.Error(ctx, "Failed to resolve host",
			"host", h.IP,
			"location", h.Location,
			"error", err,
		)
	}

	return parallel.Run(len(hosts), parallel.NoLimit, f, notify)
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
		Stage:     StageInit,
	}

	s.logger.Info(ctx, "Initializing backup",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
		"target", target,
	)

	if target.Continue {
		if err := s.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
			if err := s.clonePrevProgress(run); err != nil {
				return errors.Wrap(err, "clone progress")
			}
		}
	}

	// Generate snapshot tag
	if run.SnapshotTag == "" {
		run.SnapshotTag = backupspec.NewSnapshotTag()
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "initialize: get client proxy")
	}

	// Get live nodes
	var liveNodes scyllaclient.NodeStatusInfoSlice

	if len(run.Nodes) == 0 {
		liveNodes = target.liveNodes
		run.Nodes = liveNodes.HostIDs()
	} else {
		filter := strset.New(run.Nodes...)
		for _, v := range target.liveNodes {
			if filter.Has(v.HostID) {
				liveNodes = append(liveNodes, v)
			}
		}
		if len(liveNodes) != len(run.Nodes) {
			return errors.New("missing hosts to resume backup")
		}
	}

	// Create hostInfo for run hosts
	hi, err := makeHostInfo(liveNodes, target.Location, target.RateLimit, target.Transfers)
	if err != nil {
		return err
	}

	// Register the run
	if err := s.putRun(run); err != nil {
		return errors.Wrap(err, "initialize: register the run")
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	// Create a worker
	w := &worker{
		workerTools: workerTools{
			ClusterID:   clusterID,
			ClusterName: clusterName,
			TaskID:      taskID,
			RunID:       runID,
			SnapshotTag: run.SnapshotTag,
			Config:      s.config,
			Client:      client,
		},
		PrevStage:            run.Stage,
		Metrics:              s.metrics,
		Units:                run.Units,
		OnRunProgress:        s.putRunProgressLogError,
		ResumeUploadProgress: s.resumeUploadProgress(run.PrevID),
		memoryPool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
		dth: s.dth,
	}

	// Map stages to worker functions
	gaurdFunc := func() error {
		return nil
	}
	stageFunc := map[Stage]func() error{
		StageSnapshot: func() error {
			return w.Snapshot(ctx, hi, target.SnapshotParallel)
		},
		StageAwaitSchema: func() error {
			if !target.SkipSchema {
				return w.DumpSchema(ctx, hi, s.clusterSession)
			}
			return nil
		},
		StageIndex: func() error {
			return w.Index(ctx, hi, target.UploadParallel)
		},
		StageManifest: func() error {
			return w.UploadManifest(ctx, hi)
		},
		StageSchema: func() error {
			return w.UploadSchema(ctx, hi)
		},
		StageDeduplicate: func() error {
			return w.Deduplicate(ctx, hi, target.UploadParallel)
		},
		StageUpload: func() error {
			return w.Upload(ctx, hi, target.UploadParallel)
		},
		StageMoveManifest: func() error {
			return w.MoveManifest(ctx, hi)
		},
		StagePurge: func() error {
			return w.Purge(ctx, hi, target.RetentionMap)
		},
		StageDone: gaurdFunc,

		StageMigrate: gaurdFunc, // migrations from v1 are no longer supported
	}

	// Save the previous stage
	prevStage := run.Stage

	// Execute stages according to the stage order.
	execStage := func(stage Stage, f func() error) (err error) {
		// In purge only mode skip all stages before purge.
		if target.PurgeOnly {
			if stage.Index() < StagePurge.Index() {
				return nil
			}
		}

		// Skip completed stages
		if run.PrevID != uuid.Nil {
			// Allow re-indexing when future stages requires access
			// to indexed worker.snapshotDirs.
			allowReindexOnPrevStage := map[Stage]struct{}{
				StageManifest:    {},
				StageUpload:      {},
				StageDeduplicate: {},
				StageSchema:      {},
			}
			if _, ok := allowReindexOnPrevStage[prevStage]; ok && stage == StageIndex { //nolint: revive
				// continue
			} else if prevStage.Index() > stage.Index() {
				return nil
			}
		}

		// Prepare worker
		s.updateStage(ctx, run, stage)
		name := strings.ToLower(string(stage))
		w = w.WithLogger(s.logger.Named(name))

		// Always cleanup stats
		defer w.cleanup(ctx, hi)

		if desc, ok := stageDescription[stage]; ok {
			up := strings.ToUpper(desc[:1]) + desc[1:]
			w.Logger.Info(ctx, up+"...")
			defer func(start time.Time) {
				if err != nil {
					w.Logger.Error(ctx, up+" failed see exact errors above", "duration", timeutc.Since(start))
				} else {
					w.Logger.Info(ctx, "Done "+desc, "duration", timeutc.Since(start))
				}
			}(timeutc.Now())
		}

		// Run function
		return errors.Wrap(f(), strings.ReplaceAll(name, "_", " "))
	}
	for _, s := range StageOrder() {
		if f, ok := stageFunc[s]; ok {
			if err := execStage(s, f); err != nil {
				return err
			}
		}
	}

	return nil
}

// decorateWithPrevRun gets task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRun(ctx context.Context, run *Run) error {
	prev, err := s.GetLastResumableRun(ctx, run.ClusterID, run.TaskID)
	if errors.Is(err, util.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get previous run")
	}

	// Check if can continue from prev
	if s.config.AgeMax > 0 {
		t, err := backupspec.SnapshotTagTime(prev.SnapshotTag)
		if err != nil {
			s.logger.Info(ctx, "Starting from scratch: cannot parse snapshot tag form previous run",
				"snapshot_tag", prev.SnapshotTag,
				"prev_run_id", prev.ID,
				"error", err,
			)
			return nil
		}

		if timeutc.Since(t) > s.config.AgeMax {
			s.logger.Info(ctx, "Starting from scratch: snapshot form previous run is too old",
				"snapshot_tag", prev.SnapshotTag,
				"prev_run_id", prev.ID,
				"age_max", s.config.AgeMax,
			)
			return nil
		}
	}

	s.logger.Info(ctx, "Resuming previous run", "snapshot_tag", prev.SnapshotTag, "prev_run_id", prev.ID)

	run.PrevID = prev.ID
	run.SnapshotTag = prev.SnapshotTag
	run.Units = prev.Units
	run.DC = prev.DC
	run.Nodes = prev.Nodes
	run.Stage = prev.Stage

	return nil
}

func (s *Service) clonePrevProgress(run *Run) error {
	q := table.BackupRunProgress.InsertQuery(s.session)
	defer q.Release()

	prevRun := &Run{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}
	v := NewProgressVisitor(prevRun, s.session)
	return v.ForEach(func(p *RunProgress) error {
		p.RunID = run.ID
		return q.BindStruct(p).Exec()
	})
}

// GetLastResumableRun returns the most recent started but not done run of
// the task, if there is a recent run that is completely done ErrNotFound is
// reported.
func (s *Service) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
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
		if r.Stage == StageDone || r.Stage == stageNone {
			break
		}
		if r.Stage.Resumable() {
			return r, nil
		}
	}

	return nil, util.ErrNotFound
}

// putRun upserts a backup run.
func (s *Service) putRun(r *Run) error {
	q := table.BackupRun.InsertQuery(s.session).BindStruct(r)
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

// updateStage updates and persists run stage.
func (s *Service) updateStage(ctx context.Context, run *Run, stage Stage) {
	run.Stage = stage

	q := table.BackupRun.UpdateQuery(s.session, "stage").BindStruct(run)
	if err := q.ExecRelease(); err != nil {
		s.logger.Error(ctx, "Failed to update run stage", "error", err)
	}
}

// putRunProgress upserts a backup run progress.
func (s *Service) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	q := table.BackupRunProgress.InsertQuery(s.session).BindStruct(p)
	return q.ExecRelease()
}

// putRunProgressLogError executes putRunProgress and consumes the error.
func (s *Service) putRunProgressLogError(ctx context.Context, p *RunProgress) {
	if err := s.putRunProgress(ctx, p); err != nil {
		s.logger.Error(ctx, "Failed to update file progress", "error", err)
	}
}

func (s *Service) resumeUploadProgress(prevRunID uuid.UUID) func(context.Context, *RunProgress) {
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

// GetRun returns a run based on ID. If nothing was found scylla-manager.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
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

// GetProgress aggregates progress for the run of the task and breaks it down
// by keyspace and table.json
// If nothing was found scylla-manager.ErrNotFound is returned.
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

	switch run.Stage {
	case stageNone, StageInit, StageSnapshot, StageIndex:
		return Progress{
			SnapshotTag: run.SnapshotTag,
			DC:          run.DC,
			Stage:       run.Stage,
		}, nil
	}

	return aggregateProgress(run, NewProgressVisitor(run, s.session))
}

// DeleteSnapshot deletes backup data and meta files associated with provided snapshotTag.
func (s *Service) DeleteSnapshot(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, snapshotTags []string) error {
	s.logger.Debug(ctx, "DeleteSnapshot",
		"cluster_id", clusterID,
		"snapshot_tags", snapshotTags,
	)

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get scylla client")
	}

	// Resolve hosts for locations
	hosts := make([]hostInfo, len(locations))
	for i := range locations {
		hosts[i].Location = locations[i]
	}
	if err := s.resolveHosts(ctx, client, hosts); err != nil {
		return errors.Wrap(err, "resolve hosts")
	}

	deletedManifests := atomic.NewInt32(0)

	f := func(h hostInfo) error {
		s.logger.Info(ctx, "Purging snapshot data on host", "host", h.IP)

		manifests, err := listManifests(ctx, client, h.IP, h.Location, clusterID)
		if err != nil {
			return err
		}
		p := newPurger(client, h.IP, s.logger)

		tagS := strset.New(snapshotTags...)
		var oldest time.Time
		for _, m := range manifests {
			if tagS.Has(m.SnapshotTag) {
				continue
			}

			t, err := backupspec.SnapshotTagTime(m.SnapshotTag)
			if err != nil {
				return err
			}
			if t.Before(oldest) || oldest.IsZero() {
				oldest = t
			}
		}

		n, err := p.PurgeSnapshotTags(ctx, manifests, tagS, oldest)
		deletedManifests.Add(int32(n))

		if err == nil {
			s.logger.Info(ctx, "Done purging snapshot data on host", "host", h.IP)
		}
		return err
	}

	notify := func(h hostInfo, err error) {
		s.logger.Error(ctx, "Purging snapshot data failed on host",
			"host", h.IP,
			"error", err,
		)
	}

	if err := hostsInParallel(hosts, parallel.NoLimit, f, notify); err != nil {
		return err
	}

	if deletedManifests.Load() == 0 {
		return util.ErrNotFound
	}

	return nil
}
