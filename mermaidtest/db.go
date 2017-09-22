// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/migrate"
)

var (
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto   = flag.Int("proto", 0, "protcol version")
	flagCQL     = flag.String("cql", "3.0.0", "CQL version")
	flagRF      = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry   = flag.Int("retries", 5, "number of times to retry queries")
	flagTimeout = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	// ClusterHosts specifies addresses of nodes in a test cluster.
	ClusterHosts []string
)

func init() {
	flag.Parse()
	ClusterHosts = strings.Split(*flagCluster, ",")
}

var initOnce sync.Once

// CreateSession recreates the database and returns a new gocql.Session.
func CreateSession(tb testing.TB) *gocql.Session {
	return createSessionFromCluster(tb, createCluster())
}

func createCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(ClusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	return cluster
}

func createSessionFromCluster(tb testing.TB, cluster *gocql.ClusterConfig) *gocql.Session {
	initOnce.Do(func() {
		createKeyspace(tb, cluster, "test_scylla_management")
	})

	cluster.Keyspace = "test_scylla_management"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}

	if err := migrate.Migrate(context.Background(), session, "../schema/cql"); err != nil {
		tb.Fatal("migrate:", err)
	}

	return session
}

func createKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		tb.Fatal(err)
	}
	defer session.Close()

	mustExec(tb, session, "DROP KEYSPACE IF EXISTS "+keyspace)
	mustExec(tb, session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))
}

func mustExec(tb testing.TB, s *gocql.Session, stmt string) {
	if err := s.Query(stmt).RetryPolicy(nil).Exec(); err != nil {
		tb.Fatal("exec failed", stmt, err)
	}
}
