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
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
)

var (
	flagManagedCluster = flag.String("managed-cluster", "127.0.0.1", "a comma-separated list of host:port tuples of data cluster hosts")
	flagCluster        = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples of scylla manager db hosts")
	flagProto          = flag.Int("proto", 0, "protcol version")
	flagCQL            = flag.String("cql", "3.0.0", "CQL version")
	flagRF             = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry          = flag.Int("retries", 5, "number of times to retry queries")
	flagTimeout        = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	// ManagedClusterHosts specifies addresses of nodes in a test cluster.
	ManagedClusterHosts []string
)

func init() {
	flag.Parse()
	ManagedClusterHosts = strings.Split(*flagManagedCluster, ",")
}

var initOnce sync.Once

// CreateSession recreates the database on scylla manager cluster and returns
// a new gocql.Session.
func CreateSession(tb testing.TB) *gocql.Session {
	tb.Helper()

	session := createSessionFromCluster(tb, createCluster(*flagCluster))
	if err := migrate.Migrate(context.Background(), session, "../schema/cql"); err != nil {
		tb.Fatal("migrate:", err)
	}
	return session
}

// CreateSessionWithoutMigration clears the database on scylla manager cluster
// and returns a new gocql.Session. This is only useful for testing migrations
// you probably should be using CreateSession instead.
func CreateSessionWithoutMigration(tb testing.TB) *gocql.Session {
	tb.Helper()

	return createSessionFromCluster(tb, createCluster(*flagCluster))
}

// CreateManagedClusterSession returns a new gocql.Session to the managed data cluster.
func CreateManagedClusterSession(tb testing.TB) *gocql.Session {
	tb.Helper()

	cluster := createCluster(ManagedClusterHosts...)
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}
	dropAllKeyspaces(tb, session)
	return session
}

func createCluster(hosts ...string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
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
	tb.Helper()

	initOnce.Do(func() {
		createTestKeyspace(tb, cluster, "test_scylla_manager")
	})

	cluster.Keyspace = "test_scylla_manager"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}

	return session
}

func createTestKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	tb.Helper()

	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		tb.Fatal(err)
	}
	defer session.Close()

	dropAllKeyspaces(tb, session)

	ExecStmt(tb, session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))
}

func dropAllKeyspaces(tb testing.TB, session *gocql.Session) {
	tb.Helper()

	q := session.Query("select keyspace_name from system_schema.keyspaces")
	defer q.Release()

	var all []string
	if err := gocqlx.Select(&all, q); err != nil {
		tb.Fatal(err)
	}

	for _, k := range all {
		if !strings.HasPrefix(k, "system") {
			dropKeyspace(tb, session, k)
		}
	}
}

func dropKeyspace(tb testing.TB, session *gocql.Session, keyspace string) {
	tb.Helper()

	ExecStmt(tb, session, "DROP KEYSPACE IF EXISTS "+keyspace)
}

// ExecStmt executes given statement.
func ExecStmt(tb testing.TB, session *gocql.Session, stmt string) {
	tb.Helper()

	if err := session.Query(stmt).RetryPolicy(nil).Exec(); err != nil {
		tb.Fatal("exec failed", stmt, err)
	}
}
