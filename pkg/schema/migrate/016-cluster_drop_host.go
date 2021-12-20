// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func init() {
	reg.Add(migrate.BeforeMigration, "016-cluster_drop_host.cql", clusterMoveHostToKnownHostsBefore016)
}

func clusterMoveHostToKnownHostsBefore016(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
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
