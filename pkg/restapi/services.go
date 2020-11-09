// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"encoding/json"

	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// Services contains REST API services.
type Services struct {
	Cluster     ClusterService
	HealthCheck HealthCheckService
	Repair      RepairService
	Backup      BackupService
	Scheduler   SchedService
}

// ClusterService service interface for the REST API handlers.
type ClusterService interface {
	ListClusters(ctx context.Context, f *cluster.Filter) ([]*cluster.Cluster, error)
	GetCluster(ctx context.Context, idOrName string) (*cluster.Cluster, error)
	PutCluster(ctx context.Context, c *cluster.Cluster) error
	DeleteCluster(ctx context.Context, id uuid.UUID) error
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
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (repair.Progress, error)
	GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (repair.Target, error)
	SetIntensity(ctx context.Context, runID uuid.UUID, intensity float64) error
	SetParallel(ctx context.Context, runID uuid.UUID, parallel int) error
}

// BackupService service interface for the REST API handlers.
type BackupService interface {
	GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (backup.Target, error)
	GetTargetSize(ctx context.Context, clusterID uuid.UUID, target backup.Target) (int64, error)
	ExtractLocations(ctx context.Context, properties []json.RawMessage) []backup.Location
	List(ctx context.Context, clusterID uuid.UUID, locations []backup.Location, filter backup.ListFilter) ([]backup.ListItem, error)
	ListFiles(ctx context.Context, clusterID uuid.UUID, locations []backup.Location, filter backup.ListFilter) ([]backup.FilesInfo, error)
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (backup.Progress, error)
	DeleteSnapshot(ctx context.Context, clusterID uuid.UUID, locations []backup.Location, snapshotTag string) error
}

// SchedService service interface for the REST API handlers.
type SchedService interface {
	GetTask(ctx context.Context, clusterID uuid.UUID, tp scheduler.TaskType, idOrName string) (*scheduler.Task, error)
	PutTask(ctx context.Context, t *scheduler.Task) error
	PutTaskOnce(ctx context.Context, t *scheduler.Task) error
	DeleteTask(ctx context.Context, t *scheduler.Task) error
	ListTasks(ctx context.Context, clusterID uuid.UUID, tp scheduler.TaskType) ([]*scheduler.Task, error)
	StartTask(ctx context.Context, t *scheduler.Task, opts ...scheduler.Opt) error
	StopTask(ctx context.Context, t *scheduler.Task) error
	GetRun(ctx context.Context, t *scheduler.Task, runID uuid.UUID) (*scheduler.Run, error)
	GetLastRun(ctx context.Context, t *scheduler.Task, n int) ([]*scheduler.Run, error)
}
