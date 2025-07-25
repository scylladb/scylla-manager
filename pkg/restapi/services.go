// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"encoding/json"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/v3/pkg/service/one2onerestore"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Services contains REST API services.
type Services struct {
	Cluster        ClusterService
	HealthCheck    HealthCheckService
	Repair         RepairService
	Backup         BackupService
	Restore        RestoreService
	Scheduler      SchedService
	One2OneRestore One2OneRestoreService
}

// ClusterService service interface for the REST API handlers.
type ClusterService interface {
	ListClusters(ctx context.Context, f *cluster.Filter) ([]*cluster.Cluster, error)
	GetCluster(ctx context.Context, idOrName string) (*cluster.Cluster, error)
	PutCluster(ctx context.Context, c *cluster.Cluster) error
	DeleteCluster(ctx context.Context, id uuid.UUID) error
	CheckCQLCredentials(id uuid.UUID) (bool, error)
	DeleteCQLCredentials(ctx context.Context, id uuid.UUID) error
	DeleteSSLUserCert(ctx context.Context, id uuid.UUID) error
	ListNodes(ctx context.Context, id uuid.UUID) ([]cluster.Node, error)
}

// HealthCheckService service interface for the REST API handlers.
type HealthCheckService interface {
	Status(ctx context.Context, clusterID uuid.UUID) ([]healthcheck.NodeStatus, error)
}

// RepairService service interface for the REST API handlers.
type RepairService interface {
	GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*repair.Run, error)
	// GetProgress must work even when the cluster is no longer available.
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (repair.Progress, error)
	GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (repair.Target, error)
	SetIntensity(ctx context.Context, runID uuid.UUID, intensity float64) error
	SetParallel(ctx context.Context, runID uuid.UUID, parallel int) error
}

// BackupService service interface for the REST API handlers.
type BackupService interface {
	GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (backup.Target, error)
	GetTargetSize(ctx context.Context, clusterID uuid.UUID, target backup.Target) (int64, error)
	ExtractLocations(ctx context.Context, properties []json.RawMessage) []backupspec.Location
	List(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, filter backup.ListFilter) ([]backup.ListItem, error)
	ListFiles(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, filter backup.ListFilter) ([]backupspec.FilesInfo, error)
	// GetProgress must work even when the cluster is no longer available.
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (backup.Progress, error)
	DeleteSnapshot(ctx context.Context, clusterID uuid.UUID, locations []backupspec.Location, snapshotTags []string) error
	GetValidationTarget(_ context.Context, clusterID uuid.UUID, properties json.RawMessage) (backup.ValidationTarget, error)
	// GetValidationProgress must work even when the cluster is no longer available.
	GetValidationProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]backup.ValidationHostProgress, error)
	GetDescribeSchema(ctx context.Context, clusterID uuid.UUID, snapshotTag string, location backupspec.Location, filter backup.DescribeSchemaFilter) (query.DescribedSchema, error)
}

// RestoreService service interface for the REST API handlers.
type RestoreService interface {
	GetTargetUnitsViews(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (restore.Target, []restore.Unit, []restore.View, error)
	// GetProgress must work even when the cluster is no longer available.
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (restore.Progress, error)
}

// SchedService service interface for the REST API handlers.
type SchedService interface {
	PropertiesDecorator(tp scheduler.TaskType) scheduler.PropertiesDecorator
	GetTaskByID(ctx context.Context, clusterID uuid.UUID, tp scheduler.TaskType, id uuid.UUID) (*scheduler.Task, error)
	PutTask(ctx context.Context, t *scheduler.Task) error
	DeleteTask(ctx context.Context, t *scheduler.Task) error
	ListTasks(ctx context.Context, clusterID uuid.UUID, filter scheduler.ListFilter) ([]*scheduler.TaskListItem, error)
	StartTask(ctx context.Context, t *scheduler.Task) error
	StartTaskNoContinue(ctx context.Context, t *scheduler.Task) error
	StopTask(ctx context.Context, t *scheduler.Task) error
	GetRun(ctx context.Context, t *scheduler.Task, runID uuid.UUID) (*scheduler.Run, error)
	GetNthLastRun(ctx context.Context, t *scheduler.Task, n int) (*scheduler.Run, error)
	GetLastRuns(ctx context.Context, t *scheduler.Task, n int) ([]*scheduler.Run, error)
	IsSuspended(ctx context.Context, clusterID uuid.UUID) bool
	SuspendStatus(ctx context.Context, clusterID uuid.UUID) scheduler.SuspendStatus
	Suspend(ctx context.Context, clusterID uuid.UUID, allowTaskType string) error
	Resume(ctx context.Context, clusterID uuid.UUID, startTasks bool) error
}

// One2OneRestoreService service interface for the 1-1-restore REST API handlers.
type One2OneRestoreService interface {
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) (one2onerestore.Progress, error)
}
