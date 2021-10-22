// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type ExecFilter struct {
	Datacenter []string
	Status     scyllaclient.NodeStatus
	State      scyllaclient.NodeState
	Limit      int
}

func (s *Service) Exec(ctx context.Context, id uuid.UUID, stdin []byte, stdout io.Writer, filter ExecFilter) error {
	s.logger.Info(ctx, "Exec", "cluster_id", id, "filter", filter)

	client, err := s.Client(ctx, id)
	if err != nil {
		return errors.Wrap(err, "client proxy")
	}

	status, err := client.Status(ctx)
	if len(filter.Datacenter) != 0 {
		status = status.Datacenter(filter.Datacenter)
	}
	if filter.Status != "" {
		status = status.Status(filter.Status)
	}
	if filter.State != "" {
		status = status.State(filter.State)
	}

	return client.Exec(ctx, status.Hosts(), filter.Limit, stdin, stdout)
}
