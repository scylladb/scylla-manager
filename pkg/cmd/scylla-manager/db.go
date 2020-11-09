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
	"github.com/scylladb/scylla-manager/pkg/cmd/scylla-manager/config"
	schemamigrate "github.com/scylladb/scylla-manager/pkg/schema/migrate"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
)

func keyspaceExists(config *config.ServerConfig) (bool, error) {
	session, err := gocqlClusterConfigForDBInit(config).CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	var cnt int
	q := session.Query("SELECT COUNT(keyspace_name) FROM system_schema.keyspaces WHERE keyspace_name = ?").Bind(config.Database.Keyspace)
	return cnt == 1, q.Scan(&cnt)
}

func createKeyspace(config *config.ServerConfig) error {
	session, err := gocqlClusterConfigForDBInit(config).CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Auto upgrade replication factor if needed. RF=1 with multiple hosts means
	// data loss when one of the nodes is down. This is understood with a single
	// node deployment but must be avoided if we have more nodes.
	if config.Database.ReplicationFactor == 1 {
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
			config.Database.ReplicationFactor = rf
		}
	}

	return session.Query(mustEvaluateCreateKeyspaceStmt(config)).Exec()
}

const createKeyspaceStmt = "CREATE KEYSPACE {{.Keyspace}} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': {{.ReplicationFactor}}}"

func mustEvaluateCreateKeyspaceStmt(config *config.ServerConfig) string {
	t := template.New("")
	if _, err := t.Parse(string(createKeyspaceStmt)); err != nil {
		panic(err)
	}

	buf := new(bytes.Buffer)
	if err := t.Execute(buf, config.Database); err != nil {
		panic(err)
	}

	return buf.String()
}

func migrateSchema(config *config.ServerConfig, logger log.Logger) error {
	c := gocqlClusterConfigForDBInit(config)
	c.Keyspace = config.Database.Keyspace

	session, err := gocqlx.WrapSession(c.CreateSession())
	if err != nil {
		return err
	}
	defer session.Close()

	// Run migrations
	ctx := context.Background()
	schemamigrate.Logger = logger
	migrate.Callback = schemamigrate.Callback
	if err := migrate.Migrate(ctx, session, config.Database.MigrateDir); err != nil {
		return err
	}

	// Run post migration actions
	if err := fixSchedulerTaskTTL(session, logger, c.Keyspace); err != nil {
		return err
	}

	return nil
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
	if err := gocqlxtable.RewriteRows(session, table.SchedTask); err != nil {
		return err
	}

	return nil
}

func gocqlClusterConfigForDBInit(config *config.ServerConfig) *gocql.ClusterConfig {
	c := gocqlClusterConfig(config)
	c.Keyspace = "system"
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	// Use only a single host for migrations, using multiple hosts may lead to
	// conflicting schema changes. This can be avoided by awaiting schema
	// changes see https://github.com/scylladb/gocqlx/issues/106.
	c.Hosts = []string{config.Database.InitAddr()}
	c.DisableInitialHostLookup = true

	return c
}

func gocqlClusterConfig(config *config.ServerConfig) *gocql.ClusterConfig {
	c := gocql.NewCluster(config.Database.Hosts...)

	// Chose consistency level, for a single node deployments use ONE, for
	// multi-dc deployments use LOCAL_QUORUM, otherwise use QUORUM.
	switch {
	case config.Database.LocalDC != "":
		c.Consistency = gocql.LocalQuorum
	case config.Database.ReplicationFactor == 1:
		c.Consistency = gocql.One
	default:
		c.Consistency = gocql.Quorum
	}

	c.Keyspace = config.Database.Keyspace
	c.Timeout = config.Database.Timeout

	// SSL
	if config.Database.SSL {
		c.SslOpts = &gocql.SslOptions{
			CaPath:                 config.SSL.CertFile,
			CertPath:               config.SSL.UserCertFile,
			KeyPath:                config.SSL.UserKeyFile,
			EnableHostVerification: config.SSL.Validate,
		}
	}

	// Authentication
	if config.Database.User != "" {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Database.User,
			Password: config.Database.Password,
		}
	}

	if config.Database.TokenAware {
		fallback := gocql.RoundRobinHostPolicy()
		if config.Database.LocalDC != "" {
			fallback = gocql.DCAwareRoundRobinPolicy(config.Database.LocalDC)
		}
		c.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	}

	return c
}
