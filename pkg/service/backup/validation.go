// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/service"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/pointer"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ValidationTarget specifies parameters of location validation process.
type ValidationTarget struct {
	Location            []Location `json:"location"`
	DeleteOrphanedFiles bool       `json:"delete_orphaned_files"`
	Parallel            int        `json:"parallel"`

	liveNodes scyllaclient.NodeStatusInfoSlice
}

// ValidationRunner implements scheduler.Runner.
type ValidationRunner struct {
	service *Service
}

// Run implements scheduler.Runner.
func (r ValidationRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	t, err := r.service.GetValidationTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get validation target")
	}
	return r.service.Validate(ctx, clusterID, taskID, runID, t)
}

// ValidationRunner creates a Runner that handles backup validation.
func (s *Service) ValidationRunner() ValidationRunner {
	return ValidationRunner{service: s}
}

// GetValidationTarget converts task properties into backup ValidationTarget.
func (s *Service) GetValidationTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (ValidationTarget, error) {
	s.logger.Info(ctx, "GetValidationTarget", "cluster_id", clusterID)

	// Unmarshal and make sure location is specified
	var t ValidationTarget
	if err := json.Unmarshal(properties, &t); err != nil {
		return t, err
	}
	if t.Location == nil {
		return t, errors.Errorf("missing location")
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrap(err, "get client proxy")
	}

	// Validate validation target and get target nodes...
	liveNodes, err := s.checkValidationTarget(ctx, client, t)
	if err != nil {
		return t, err
	}
	t.liveNodes = liveNodes

	return t, nil
}

// validationRunProgress embeds ValidationResult with task run information.
type validationRunProgress struct {
	ClusterID   uuid.UUID
	TaskID      uuid.UUID
	RunID       uuid.UUID
	DC          string
	Host        string
	Location    Location
	Manifests   int
	StartedAt   *time.Time
	CompletedAt *time.Time
	ValidationResult
}

// Validate checks that all SSTable files that are referenced in manifests are
// present. It also checks there are no additional files that somehow leaked
// the purging process. If it finds such files there are removed.
//
// The process is based on listing all files in SSTable directories. This is
// done in parallel, each node works with its data.
func (s *Service) Validate(ctx context.Context, clusterID, taskID, runID uuid.UUID, target ValidationTarget) error {
	s.logger.Info(ctx, "Validate",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get client proxy")
	}

	if len(target.liveNodes) == 0 {
		target.liveNodes, err = s.checkValidationTarget(ctx, client, target)
		if err != nil {
			return err
		}
	}

	hosts, err := makeHostInfo(target.liveNodes, target.Location, nil)
	if err != nil {
		return err
	}

	// List manifests in all locations
	manifests, err := listManifestsInAllLocations(ctx, client, hosts, clusterID)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	// Get a nodeID manifests popping function
	pop := popNodeIDManifestsForLocation(manifests)

	var (
		brokenSnapshots = strset.New()
		orphanedFiles   int
		mu              sync.Mutex
	)

	if err := hostsInParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		var (
			nodeID    string
			manifests []*ManifestInfo
			progress  validationRunProgress
		)

		p := newPurger(client, h.IP, log.NopLogger)

		hostForNodeID := func() string {
			for _, h := range hosts {
				if h.ID == nodeID {
					return h.IP
				}
			}
			if host := p.Host(nodeID); host != "" {
				return host
			}
			return nodeID
		}

		putProgress := func() {
			if progress.Host == "" {
				progress.Host = hostForNodeID()
			}
			if err := s.putValidationRunProgress(progress); err != nil {
				s.logger.Error(ctx, "Failed to put validation result", "error", err)
			}
		}

		p.OnScan = func(scanned, orphaned int, orphanedBytes int64) {
			progress.ScannedFiles = scanned
			progress.OrphanedFiles = orphaned
			progress.OrphanedBytes = orphanedBytes
			putProgress()
		}
		p.OnDelete = func(total, success int) {
			progress.DeletedFiles = success
			putProgress()
		}

		f := func(nodeID string, manifests []*ManifestInfo) error {
			var logger log.Logger
			if nodeID == h.ID {
				logger = s.logger.Named("validate").With("host", h.IP)
				logger.Info(ctx, "Validating host snapshots")
				defer logger.Info(ctx, "Done validating host snapshots")
			} else {
				logger = s.logger.Named("validate").With(
					"host", h.IP,
					"node", nodeID,
				)
				logger.Info(ctx, "Validating node snapshots from host")
				defer logger.Info(ctx, "Done validating node snapshots from host")
			}
			p.logger = logger

			progress = validationRunProgress{
				ClusterID: clusterID,
				TaskID:    taskID,
				RunID:     runID,
				DC:        h.DC,
				// Defer host assignment until we get to know the host IP.
				// We can assign early for known hosts but in general case it's
				// read from manifest. This is implemented in putProgress
				// and hostForNodeID but requires reading manifests first.
				// Host
				Location:  h.Location,
				Manifests: len(manifests),
				StartedAt: pointer.TimePtr(timeutc.Now()),
			}
			if h.ID == nodeID {
				putProgress()
			}
			defer func() {
				progress.CompletedAt = pointer.TimePtr(timeutc.Now())
				putProgress()
			}()

			v, err := p.Validate(ctx, manifests, target.DeleteOrphanedFiles)
			progress.ValidationResult = v

			// Aggregate results
			mu.Lock()
			brokenSnapshots.Add(v.BrokenSnapshots...)
			orphanedFiles += v.OrphanedFiles
			mu.Unlock()

			return err
		}

		for {
			// Get node to validate in the same location, if cannot find any exit
			nodeID, manifests = pop(h)
			if len(manifests) == 0 {
				return nil
			}

			if err := f(nodeID, manifests); err != nil {
				return err
			}
		}
	}); err != nil {
		return err
	}

	var msg []string
	if !brokenSnapshots.IsEmpty() {
		bs := brokenSnapshots.List()
		sort.Strings(bs)
		msg = append(msg, fmt.Sprintf("broken snapshots: %s", strings.Join(bs, ", ")))
	}
	if !target.DeleteOrphanedFiles && orphanedFiles > 0 {
		msg = append(msg, "orphaned files")
	}

	if len(msg) > 0 {
		return errors.New(strings.Join(msg, " and "))
	}

	return nil
}

func (s *Service) checkValidationTarget(ctx context.Context, client *scyllaclient.Client, target ValidationTarget) (scyllaclient.NodeStatusInfoSlice, error) {
	// Get live nodes
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get result")
	}
	liveNodes := status.Live()

	// Filter by DC if needed
	var dcs []string
	for _, l := range target.Location {
		if l.DC != "" {
			dcs = append(dcs, l.DC)
		}
	}
	if len(dcs) > 0 {
		liveNodes = liveNodes.Datacenter(dcs)
	}

	// Validate locations access
	if len(liveNodes) == 0 {
		return nil, service.ErrValidate(errors.Errorf("wrong location"))
	}
	if err := s.checkLocationsAvailableFromNodes(ctx, client, liveNodes, target.Location); err != nil {
		return nil, service.ErrValidate(errors.Wrap(err, "location is not accessible"))
	}

	return liveNodes, nil
}

func (s *Service) putValidationRunProgress(p validationRunProgress) error {
	return table.ValidateBackupRunProgress.InsertQuery(s.session).BindStruct(p).ExecRelease()
}

// ValidationHostProgress represents validation results per host.
type ValidationHostProgress struct {
	DC          string     `json:"dc"`
	Host        string     `json:"host"`
	Location    Location   `json:"location"`
	Manifests   int        `json:"manifests"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	ValidationResult
}

// GetValidationProgress returns the current validation result.
func (s *Service) GetValidationProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]ValidationHostProgress, error) {
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
