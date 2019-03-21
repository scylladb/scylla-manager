// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/uuid"
)

func init() {
	h := clusterMoveHostsToHost011{
		m: make(map[uuid.UUID]string),
	}
	registerMigrationCallback("011-cluster_add_host.cql", migrate.BeforeMigration, h.Before)
	registerMigrationCallback("011-cluster_add_host.cql", migrate.AfterMigration, h.After)
}

type clusterMoveHostsToHost011 struct {
	m map[uuid.UUID]string
}

func (h clusterMoveHostsToHost011) Before(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	type cluster struct {
		ID    uuid.UUID
		Hosts []string
	}
	stmt, names := qb.Select("cluster").Columns("id", "hosts").ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
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

func (h clusterMoveHostsToHost011) After(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	const updateClusterCql = `INSERT INTO cluster(id, host) VALUES (?, ?)`
	q := session.Query(updateClusterCql)
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
