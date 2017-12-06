// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"sort"
	"strconv"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient/internal/client/operations"
	"github.com/scylladb/mermaid/mermaidclient/internal/models"
)

//go:generate ./gen_internal.sh

// Client provides means to interact with Mermaid.
type Client struct {
	operations *operations.Client
}

// NewClient creates a new client.
func NewClient(rawurl string) (Client, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return Client{}, err
	}

	return Client{operations: operations.New(api.New(u.Host, u.Path, []string{u.Scheme}), strfmt.Default)}, nil
}

// CreateCluster creates a new cluster.
func (c Client) CreateCluster(ctx context.Context, cluster *Cluster) (string, error) {
	resp, err := c.operations.PostClusters(&operations.PostClustersParams{
		Context: ctx,
		Cluster: cluster,
	})
	if err != nil {
		return "", err
	}

	clusterID, err := extractIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return clusterID.String(), nil
}

// GetCluster returns a cluster for a given ID.
func (c Client) GetCluster(ctx context.Context, clusterID string) (*Cluster, error) {

	resp, err := c.operations.GetClusterClusterID(&operations.GetClusterClusterIDParams{
		Context:   ctx,
		ClusterID: clusterID,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// UpdateCluster updates cluster.
func (c Client) UpdateCluster(ctx context.Context, cluster *Cluster) error {
	_, err := c.operations.PutClusterClusterID(&operations.PutClusterClusterIDParams{
		Context:   ctx,
		ClusterID: cluster.ID,
		Cluster:   cluster,
	})
	return err
}

// DeleteCluster removes cluster.
func (c Client) DeleteCluster(ctx context.Context, clusterID string) error {
	_, err := c.operations.DeleteClusterClusterID(&operations.DeleteClusterClusterIDParams{
		Context:   ctx,
		ClusterID: clusterID,
	})
	return err
}

// ListClusters returns clusters.
func (c Client) ListClusters(ctx context.Context) ([]*Cluster, error) {
	resp, err := c.operations.GetClusters(&operations.GetClustersParams{
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// RepairProgress returns repair progress.
func (c Client) RepairProgress(ctx context.Context, clusterID, unitID, taskID string) (status string, progress int, rows []RepairProgressRow, err error) {
	params := &operations.GetClusterClusterIDRepairUnitUnitIDProgressParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unitID,
	}
	if taskID != "" {
		params.TaskID = &taskID
	}

	resp, err := c.operations.GetClusterClusterIDRepairUnitUnitIDProgress(params)
	if err != nil {
		return
	}

	for host, h := range resp.Payload.Hosts {
		ip := net.ParseIP(host)
		if ip == nil {
			err = errors.Wrap(err, "cannot parse response")
			return
		}

		if h.Total == 0 {
			rows = append(rows, RepairProgressRow{
				Host:  ip,
				Shard: -1,
			})
			continue
		}

		var shard int64
		for shardStr, s := range h.Shards {
			shard, err = strconv.ParseInt(shardStr, 10, 64)
			if err != nil {
				err = errors.Wrap(err, "cannot parse response")
				return
			}

			rows = append(rows, RepairProgressRow{
				Host:     ip,
				Shard:    int(shard),
				Progress: int(s.PercentComplete),
				Error:    int(s.Error),
			})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		switch bytes.Compare(rows[i].Host, rows[j].Host) {
		case -1:
			return true
		case 0:
			return rows[i].Shard < rows[j].Shard
		default:
			return false
		}
	})

	status = resp.Payload.Status
	progress = int(resp.Payload.PercentComplete)
	return
}

// CreateRepairUnit creates a new repair unit.
func (c Client) CreateRepairUnit(ctx context.Context, u *RepairUnit) (string, error) {
	resp, err := c.operations.PostClusterClusterIDRepairUnits(&operations.PostClusterClusterIDRepairUnitsParams{
		Context:   ctx,
		ClusterID: u.ClusterID,
		UnitFields: &models.RepairUnitUpdate{
			Name:     u.Name,
			Keyspace: u.Keyspace,
			Tables:   u.Tables,
		},
	})
	if err != nil {
		return "", err
	}

	unitID, err := extractIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return unitID.String(), nil
}

// GetRepairUnit returns a repair unit for a given ID.
func (c Client) GetRepairUnit(ctx context.Context, clusterID, unitID string) (*RepairUnit, error) {
	resp, err := c.operations.GetClusterClusterIDRepairUnitUnitID(&operations.GetClusterClusterIDRepairUnitUnitIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unitID,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// UpdateRepairUnit updates existing repair unit.
func (c Client) UpdateRepairUnit(ctx context.Context, u *RepairUnit) error {
	_, err := c.operations.PutClusterClusterIDRepairUnitUnitID(&operations.PutClusterClusterIDRepairUnitUnitIDParams{
		Context:   ctx,
		ClusterID: u.ClusterID,
		UnitID:    u.ID,
		UnitFields: &models.RepairUnitUpdate{
			Name:     u.Name,
			Keyspace: u.Keyspace,
			Tables:   u.Tables,
		},
	})
	return err
}

// DeleteRepairUnit removes existing repair unit.
func (c Client) DeleteRepairUnit(ctx context.Context, clusterID, unitID string) error {
	_, err := c.operations.DeleteClusterClusterIDRepairUnitUnitID(&operations.DeleteClusterClusterIDRepairUnitUnitIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unitID,
	})
	return err
}

// ListRepairUnits returns repair units within a clusterID.
func (c Client) ListRepairUnits(ctx context.Context, clusterID string) ([]*RepairUnit, error) {
	resp, err := c.operations.GetClusterClusterIDRepairUnits(&operations.GetClusterClusterIDRepairUnitsParams{
		Context:   ctx,
		ClusterID: clusterID,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// Version returns server version.
func (c Client) Version(ctx context.Context) (*models.Version, error) {
	resp, err := c.operations.GetVersion(&operations.GetVersionParams{
		Context: ctx,
	})
	if err != nil {
		return &models.Version{}, err
	}

	return resp.Payload, nil
}

// CreateSchedTask creates a new task.
func (c *Client) CreateSchedTask(ctx context.Context, clusterID string, t *Task) (string, error) {
	resp, err := c.operations.PostClusterClusterIDTasks(&operations.PostClusterClusterIDTasksParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskFields: &models.TaskUpdate{
			Type:       t.Type,
			Enabled:    t.Enabled,
			Metadata:   t.Metadata,
			Name:       t.Name,
			Schedule:   t.Schedule,
			Tags:       t.Tags,
			Properties: t.Properties,
		},
	})
	if err != nil {
		return "", err
	}

	taskID, err := extractIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return taskID.String(), nil
}

// GetSchedTask returns a task of a given type and ID.
func (c *Client) GetSchedTask(ctx context.Context, clusterID string, tp string, taskID string) (*Task, error) {
	resp, err := c.operations.GetClusterClusterIDTaskTaskTypeTaskID(&operations.GetClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  tp,
		TaskID:    taskID,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// SchedStartTask starts executing a task.
func (c *Client) SchedStartTask(ctx context.Context, clusterID string, tp string, taskID string) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskIDStart(&operations.PutClusterClusterIDTaskTaskTypeTaskIDStartParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  tp,
		TaskID:    taskID,
	})

	return err
}

// SchedStopTask stops executing a task.
func (c *Client) SchedStopTask(ctx context.Context, clusterID string, tp string, taskID string) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskIDStop(&operations.PutClusterClusterIDTaskTaskTypeTaskIDStopParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  tp,
		TaskID:    taskID,
	})

	return err
}

// SchedDeleteTask stops executing a task.
func (c *Client) SchedDeleteTask(ctx context.Context, clusterID string, tp string, taskID string) error {
	_, err := c.operations.DeleteClusterClusterIDTaskTaskTypeTaskID(&operations.DeleteClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  tp,
		TaskID:    taskID,
	})

	return err
}

// UpdateTask updates an existing task unit.
func (c *Client) UpdateTask(ctx context.Context, clusterID string, tp string, taskID string, t *Task) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskID(&operations.PutClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  tp,
		TaskID:    taskID,
		TaskFields: &models.TaskUpdate{
			Enabled:    t.Enabled,
			Metadata:   t.Metadata,
			Name:       t.Name,
			Schedule:   t.Schedule,
			Tags:       t.Tags,
			Properties: t.Properties,
		},
	})
	return err
}

// ListSchedTasks returns scheduled tasks within a clusterID, optionaly filtered by task type tp.
func (c *Client) ListSchedTasks(ctx context.Context, clusterID string, tp string) ([]*Task, error) {
	resp, err := c.operations.GetClusterClusterIDTasks(&operations.GetClusterClusterIDTasksParams{
		Context:   ctx,
		ClusterID: clusterID,
		Type:      &tp,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}
