// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func init() {
	h := clusterMoveHostsToHost011{
		m: make(map[uuid.UUID]string),
	}
	reg.Add(migrate.BeforeMigration, "011-cluster_add_host.cql", h.Before)
	reg.Add(migrate.AfterMigration, "011-cluster_add_host.cql", h.After)
}

type clusterMoveHostsToHost011 struct {
	m map[uuid.UUID]string
}

func (h clusterMoveHostsToHost011) Before(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	type cluster struct {
		ID    uuid.UUID
		Hosts []string
	}
	q := qb.Select("cluster").Columns("id", "hosts").Query(session)
	var clusters []*cluster
	if err := q.SelectRelease(&clusters); err != nil {
		return err
	}

	for _, c := range clusters {
		if len(c.Hosts) > 0 {
			h.m[c.ID] = c.Hosts[0]
		}
	}
	return nil
}

func (h clusterMoveHostsToHost011) After(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	const updateClusterCql = `INSERT INTO cluster(id, host) VALUES (?, ?)`
	q := session.Query(updateClusterCql, nil)
	defer q.Release()

	for id, host := range h.m {
		if err := q.Bind(id, host).Exec(); err != nil {
			return err
		}
		// Ensure the state is cleared, when reran in integration tests,
		// it shall not insert any clusters.
		delete(h.m, id)
	}

	return nil
}
