// Copyright (C) 2017 ScyllaDB

package main

import (
	"bytes"
	"context"
	"text/template"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	gocqlxtable "github.com/scylladb/gocqlx/v2/table"
	"github.com/scylladb/scylla-manager/pkg/config"
	schemamigrate "github.com/scylladb/scylla-manager/pkg/schema/migrate"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
)

func keyspaceExists(c config.ServerConfig) (bool, error) {
	session, err := gocqlClusterConfigForDBInit(c).CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	var cnt int
	q := session.Query("SELECT COUNT(keyspace_name) FROM system_schema.keyspaces WHERE keyspace_name = ?").Bind(c.Database.Keyspace)
	return cnt == 1, q.Scan(&cnt)
}

func createKeyspace(c config.ServerConfig) error {
	session, err := gocqlClusterConfigForDBInit(c).CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Auto upgrade replication factor if needed. RF=1 with multiple hosts means
	// data loss when one of the nodes is down. This is understood with a single
	// node deployment but must be avoided if we have more nodes.
	if c.Database.ReplicationFactor == 1 {
		var peers int
		q := session.Query("SELECT COUNT(*) FROM system.peers")
		if err := q.Scan(&peers); err != nil {
			return err
		}
		if peers > 0 {
			rf := peers + 1
			if rf > 3 {
				rf = 3
			}
			c.Database.ReplicationFactor = rf
		}
	}

	return session.Query(mustEvaluateCreateKeyspaceStmt(c)).Exec()
}

const createKeyspaceStmt = "CREATE KEYSPACE {{.Keyspace}} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': {{.ReplicationFactor}}}"

func mustEvaluateCreateKeyspaceStmt(c config.ServerConfig) string {
	t := template.New("")
	if _, err := t.Parse(createKeyspaceStmt); err != nil {
		panic(err)
	}

	buf := new(bytes.Buffer)
	if err := t.Execute(buf, c.Database); err != nil {
		panic(err)
	}

	return buf.String()
}

func migrateSchema(c config.ServerConfig, logger log.Logger) error {
	cluster := gocqlClusterConfigForDBInit(c)
	cluster.Keyspace = c.Database.Keyspace

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return err
	}
	defer session.Close()

	// Run migrations
	ctx := context.Background()
	schemamigrate.Logger = logger
	migrate.Callback = schemamigrate.Callback
	if err := migrate.Migrate(ctx, session, c.Database.MigrateDir); err != nil {
		return err
	}

	// Run post migration actions
	return fixSchedulerTaskTTL(session, logger, cluster.Keyspace)
}

func fixSchedulerTaskTTL(session gocqlx.Session, logger log.Logger, keyspace string) error {
	ctx := context.Background()

	var ttl int
	if err := session.Query("SELECT default_time_to_live FROM system_schema.tables WHERE keyspace_name=? AND table_name=?", nil).
		Bind(keyspace, "scheduler_task").Scan(&ttl); err != nil {
		logger.Info(ctx, "Failed to get scheduler_task table properties")
	}
	if ttl == 0 {
		return nil
	}

	logger.Info(ctx, "Post migration", "action", "fix TTL in scheduler_task table")
	if err := session.ExecStmt("ALTER TABLE scheduler_task WITH default_time_to_live = 0"); err != nil {
		return err
	}

	return gocqlxtable.RewriteRows(session, table.SchedTask)
}

func gocqlClusterConfigForDBInit(c config.ServerConfig) *gocql.ClusterConfig {
	cluster := gocqlClusterConfig(c)
	cluster.Keyspace = "system"
	cluster.Timeout = c.Database.MigrateTimeout
	cluster.MaxWaitSchemaAgreement = c.Database.MigrateMaxWaitSchemaAgreement

	// Use only a single host for migrations, using multiple hosts may lead to
	// conflicting schema changes. This can be avoided by awaiting schema
	// changes see https://github.com/scylladb/gocqlx/issues/106.
	cluster.Hosts = []string{c.Database.InitAddr}
	cluster.DisableInitialHostLookup = true

	return cluster
}

func gocqlClusterConfig(c config.ServerConfig) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(c.Database.Hosts...)

	// Chose consistency level, for a single node deployments use ONE, for
	// multi-dc deployments use LOCAL_QUORUM, otherwise use QUORUM.
	switch {
	case c.Database.LocalDC != "":
		cluster.Consistency = gocql.LocalQuorum
	case c.Database.ReplicationFactor == 1:
		cluster.Consistency = gocql.One
	default:
		cluster.Consistency = gocql.Quorum
	}

	cluster.Keyspace = c.Database.Keyspace
	cluster.Timeout = c.Database.Timeout

	// SSL
	if c.Database.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 c.SSL.CertFile,
			CertPath:               c.SSL.UserCertFile,
			KeyPath:                c.SSL.UserKeyFile,
			EnableHostVerification: c.SSL.Validate,
		}
	}

	// Authentication
	if c.Database.User != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: c.Database.User,
			Password: c.Database.Password,
		}
	}

	if c.Database.TokenAware {
		fallback := gocql.RoundRobinHostPolicy()
		if c.Database.LocalDC != "" {
			fallback = gocql.DCAwareRoundRobinPolicy(c.Database.LocalDC)
		}
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	}

	return cluster
}
