// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func init() {
	registerCallback("016-cluster_drop_host.cql", migrate.BeforeMigration, clusterMoveHostToKnownHostsBefore016)
}

func clusterMoveHostToKnownHostsBefore016(ctx context.Context, session gocqlx.Session, logger log.Logger) error {
	type cluster struct {
		ID         uuid.UUID
		Host       string
		KnownHosts []string
	}

	q := qb.Select("cluster").Columns("id", "host", "known_hosts").Query(session)

	var clusters []*cluster
	if err := q.SelectRelease(&clusters); err != nil {
		return err
	}

	const updateClusterCql = `INSERT INTO cluster(id, known_hosts) VALUES (?, ?)`
	u := session.Query(updateClusterCql, nil)
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
