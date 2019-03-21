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
	registerMigrationCallback("016-cluster_drop_host.cql", migrate.BeforeMigration, clusterMoveHostToKnownHostsBefore016)
}

func clusterMoveHostToKnownHostsBefore016(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	type cluster struct {
		ID         uuid.UUID
		Host       string
		KnownHosts []string
	}

	stmt, names := qb.Select("cluster").Columns("id", "host", "known_hosts").ToCql()
	q := gocqlx.Query(session.Query(stmt), names)

	var clusters []*cluster
	if err := q.SelectRelease(&clusters); err != nil {
		return err
	}

	const updateClusterCql = `INSERT INTO cluster(id, known_hosts) VALUES (?, ?)`
	u := session.Query(updateClusterCql)
	defer q.Release()

	for _, c := range clusters {
		if len(c.KnownHosts) == 0 {
			if err := u.Bind(c.ID, []string{c.Host}).Exec(); err != nil {
				return err
			}
		}
	}

	return nil
}
