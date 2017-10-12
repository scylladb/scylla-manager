// Copyright (C) 2017 ScyllaDB

package mermaid

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
	"github.com/scylladb/mermaid/command/client/mermaid/internal/client/operations"
	"github.com/scylladb/mermaid/uuid"
)

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

// StartRepair starts a repair of a unit.
func (c *Client) StartRepair(ctx context.Context, unit string) (uuid.UUID, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid cluster")
	}

	if err := isUUID(unit); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid unit")
	}

	resp, err := c.operations.PutClusterClusterIDRepairUnitUnitIDRepair(&operations.PutClusterClusterIDRepairUnitUnitIDRepairParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
	})
	if err != nil {
		return uuid.Nil, errors.Wrapf(err, "host %s", c.host)
	}

	u, err := extractUUIDFromLocation(resp.Location)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "cannot parse response")
	}

	return u, nil
}

// StopRepair stops a repair of a unit.
func (c *Client) StopRepair(ctx context.Context, unit string) (uuid.UUID, error) {
	clusterID, err := c.clusterUUID()
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid cluster")
	}

	if err := isUUID(unit); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid unit")
	}

	resp, err := c.operations.PutClusterClusterIDRepairUnitUnitIDStopRepair(&operations.PutClusterClusterIDRepairUnitUnitIDStopRepairParams{
		Context:   ctx,
		ClusterID: clusterID,
		UnitID:    unit,
	})
	if err != nil {
		return uuid.Nil, errors.Wrapf(err, "host %s", c.host)
	}

	u, err := extractUUIDFromLocation(resp.Location)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "cannot parse response")
	}

	return u, nil
}

// RepairProgress returns progress of a repair.
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
		err = errors.Wrapf(err, "host %s", c.host)
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

// ListRepairUnits lists repair units within a cluster
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
		return nil, errors.Wrapf(err, "host %s", c.host)
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
