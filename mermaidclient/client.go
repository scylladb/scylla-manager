// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient/internal/client/operations"
	"github.com/scylladb/mermaid/mermaidclient/internal/models"
	"github.com/scylladb/mermaid/uuid"
)

var disableOpenAPIDebugOnce sync.Once

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

	disableOpenAPIDebugOnce.Do(func() {
		middleware.Debug = false
	})

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	r := api.NewWithClient(u.Host, u.Path, []string{u.Scheme}, httpClient)
	// debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	r.Debug = false

	return Client{operations: operations.New(r, strfmt.Default)}, nil
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

	clusterID, err := uuidFromLocation(resp.Location)
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
func (c Client) RepairProgress(ctx context.Context, clusterID, taskID, runID string) (status, cause string, progress int, rows []RepairProgressRow, err error) {
	var resp *operations.GetClusterClusterIDTaskRepairTaskIDRunIDProgressOK
	resp, err = c.operations.GetClusterClusterIDTaskRepairTaskIDRunIDProgress(&operations.GetClusterClusterIDTaskRepairTaskIDRunIDProgressParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskID:    taskID,
		RunID:     runID,
	})
	if err != nil {
		return
	}

	for host, h := range resp.Payload.Hosts {
		ip := net.ParseIP(host)
		if ip == nil {
			err = errors.Wrap(err, "cannot parse response")
			return
		}

		rows = append(rows, RepairProgressRow{
			Host:     ip,
			Shard:    -1,
			Progress: int(h.PercentComplete),
			Error:    int(h.Error),
			Empty:    h.Total == 0,
		})

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
				Empty:    h.Total == 0,
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
	cause = resp.Payload.Cause
	progress = int(resp.Payload.PercentComplete)

	return
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

// CreateTask creates a new task.
func (c *Client) CreateTask(ctx context.Context, clusterID string, t *Task) (uuid.UUID, error) {
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
		return uuid.Nil, err
	}

	taskID, err := uuidFromLocation(resp.Location)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "cannot parse response")
	}

	return taskID, nil
}

// GetTask returns a task of a given type and ID.
func (c *Client) GetTask(ctx context.Context, clusterID, taskType string, taskID uuid.UUID) (*Task, error) {
	resp, err := c.operations.GetClusterClusterIDTaskTaskTypeTaskID(&operations.GetClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// GetTaskHistory returns a run history of task of a given type and task ID.
func (c *Client) GetTaskHistory(ctx context.Context, clusterID, taskType string, taskID uuid.UUID, limit int) ([]*TaskRun, error) {
	params := &operations.GetClusterClusterIDTaskTaskTypeTaskIDHistoryParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
	}

	l := int32(limit)
	params.Limit = &l

	resp, err := c.operations.GetClusterClusterIDTaskTaskTypeTaskIDHistory(params)
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// StartTask starts executing a task.
func (c *Client) StartTask(ctx context.Context, clusterID, taskType string, taskID uuid.UUID) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskIDStart(&operations.PutClusterClusterIDTaskTaskTypeTaskIDStartParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
	})

	return err
}

// StopTask stops executing a task.
func (c *Client) StopTask(ctx context.Context, clusterID, taskType string, taskID uuid.UUID) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskIDStop(&operations.PutClusterClusterIDTaskTaskTypeTaskIDStopParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
	})

	return err
}

// DeleteTask stops executing a task.
func (c *Client) DeleteTask(ctx context.Context, clusterID, taskType string, taskID uuid.UUID) error {
	_, err := c.operations.DeleteClusterClusterIDTaskTaskTypeTaskID(&operations.DeleteClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
	})

	return err
}

// UpdateTask updates an existing task unit.
func (c *Client) UpdateTask(ctx context.Context, clusterID, taskType string, taskID uuid.UUID, t *Task) error {
	_, err := c.operations.PutClusterClusterIDTaskTaskTypeTaskID(&operations.PutClusterClusterIDTaskTaskTypeTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		TaskType:  taskType,
		TaskID:    taskID.String(),
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

// ListTasks returns uled tasks within a clusterID, optionaly filtered by task type tp.
func (c *Client) ListTasks(ctx context.Context, clusterID, taskType string, all bool, status string) ([]*ExtendedTask, error) {
	resp, err := c.operations.GetClusterClusterIDTasks(&operations.GetClusterClusterIDTasksParams{
		Context:   ctx,
		ClusterID: clusterID,
		Type:      &taskType,
		All:       &all,
		Status:    &status,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}
