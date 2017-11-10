// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"path"
	"sort"
	"strconv"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient/internal/client/operations"
	"github.com/scylladb/mermaid/mermaidclient/internal/models"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate ./gen_internal.sh

// Client provides means to interact with Mermaid.
type Client struct {
	operations *operations.Client
	host       string
	cluster    string
}

// NewClient creates a new client.
func NewClient(host string, cluster string) *Client {
	return &Client{
		operations: operations.New(api.New(host, "/api/v1", []string{"http"}), strfmt.Default),
		host:       host,
		cluster:    cluster,
	}
}

// StartRepair starts unit repair.
func (c *Client) StartRepair(ctx context.Context, unit string) (string, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return "", errors.Wrap(err, "invalid cluster")
	}

	if err := isUUID(unit); err != nil {
		return "", errors.Wrap(err, "invalid unit")
	}

	resp, err := c.operations.PutClusterClusterIDRepairUnitUnitIDRepair(&operations.PutClusterClusterIDRepairUnitUnitIDRepairParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
	})
	if err != nil {
		return "", err
	}

	taskID, err := extractUUIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return taskID.String(), nil
}

// StopRepair stops unit repair.
func (c *Client) StopRepair(ctx context.Context, unit string) (string, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return "", errors.Wrap(err, "invalid cluster")
	}

	if err := isUUID(unit); err != nil {
		return "", errors.Wrap(err, "invalid unit")
	}

	resp, err := c.operations.PutClusterClusterIDRepairUnitUnitIDStopRepair(&operations.PutClusterClusterIDRepairUnitUnitIDStopRepairParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
	})
	if err != nil {
		return "", err
	}

	taskID, err := extractUUIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return taskID.String(), nil
}

// RepairProgress returns repair progress.
func (c *Client) RepairProgress(ctx context.Context, unit string, task string) (status string, progress int, rows []RepairProgressRow, err error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		err = errors.Wrap(err, "invalid cluster")
		return
	}

	err = isUUID(unit)
	if err != nil {
		err = errors.Wrap(err, "invalid unit")
		return
	}

	err = isUUID(task)
	if err != nil {
		err = errors.Wrap(err, "invalid task")
		return
	}

	resp, err := c.operations.GetClusterClusterIDRepairTaskTaskID(&operations.GetClusterClusterIDRepairTaskTaskIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
		TaskID:    task,
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
func (c *Client) CreateRepairUnit(ctx context.Context, u *RepairUnit) (string, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return "", errors.Wrap(err, "invalid cluster")
	}

	resp, err := c.operations.PostClusterClusterIDRepairUnits(&operations.PostClusterClusterIDRepairUnitsParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitFields: &models.RepairUnitUpdate{
			Keyspace: u.Keyspace,
			Tables:   u.Tables,
		},
	})
	if err != nil {
		return "", err
	}

	unitID, err := extractUUIDFromLocation(resp.Location)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse response")
	}

	return unitID.String(), nil
}

// UpdateRepairUnit updates existing repair unit.
func (c *Client) UpdateRepairUnit(ctx context.Context, unit string, u *RepairUnit) error {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	if err = isUUID(unit); err != nil {
		return errors.Wrap(err, "invalid unit")
	}

	_, err = c.operations.PutClusterClusterIDRepairUnitUnitID(&operations.PutClusterClusterIDRepairUnitUnitIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
		UnitFields: &models.RepairUnitUpdate{
			Keyspace: u.Keyspace,
			Tables:   u.Tables,
		},
	})
	return err
}

// GetRepairUnit returns a repair unit for a given ID.
func (c *Client) GetRepairUnit(ctx context.Context, unit string) (*RepairUnit, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return nil, errors.Wrap(err, "invalid cluster")
	}

	if err = isUUID(unit); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	resp, err := c.operations.GetClusterClusterIDRepairUnitUnitID(&operations.GetClusterClusterIDRepairUnitUnitIDParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// ListRepairUnits returns repair units within a cluster.
func (c *Client) ListRepairUnits(ctx context.Context) ([]*RepairUnit, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return nil, errors.Wrap(err, "invalid cluster")
	}

	resp, err := c.operations.GetClusterClusterIDRepairUnits(&operations.GetClusterClusterIDRepairUnitsParams{
		Context:   ctx,
		ClusterID: clusterID,
	})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// clusterUUID returns cluster UUID for cluster name (alias resolution is not
// yet implemented).
func (c *Client) clusterUUID() (string, error) {
	var u uuid.UUID
	err := u.UnmarshalText([]byte(c.cluster))

	if err == nil {
		return u.String(), nil
	}

	return "", err
}

func isUUID(str string) error {
	var u uuid.UUID
	return u.UnmarshalText([]byte(str))
}

func extractUUIDFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	_, id := path.Split(l.Path)

	var u uuid.UUID
	if err := u.UnmarshalText([]byte(id)); err != nil {
		return uuid.Nil, err
	}

	return u, nil
}
