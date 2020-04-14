// Copyright (C) 2017 ScyllaDB

package testutils

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
	flagCluster  = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples of scylla manager db hosts")
	flagUser     = flag.String("user", "", "CQL user")
	flagPassword = flag.String("password", "", "CQL password")

	flagManagedCluster = flag.String("managed-cluster", "127.0.0.1", "a comma-separated list of host:port tuples of data cluster hosts")
	flagSchemaDir      = flag.String("schema-dir", "", "path to schema dir")
)

// ManagedClusterHosts specifies addresses of nodes in a test cluster.
func ManagedClusterHosts() []string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return strings.Split(*flagManagedCluster, ",")
}

// ManagedClusterHost returns ManagedClusterHosts()[0]
func ManagedClusterHost() string {
	s := ManagedClusterHosts()
	if len(s) == 0 {
		panic("No nodes specified in --managed-cluster flag")
	}
	return s[0]
}

// ManagedClusterCredentials returns CQL username and password.
func ManagedClusterCredentials() (user, password string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagUser, *flagPassword
}

// SchemaDir returns the specified location of CQL files.
func SchemaDir() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagSchemaDir
}

var initOnce sync.Once

// CreateSession recreates the database on scylla manager cluster and returns
// a new gocql.Session.
func CreateSession(tb testing.TB) *gocql.Session {
	tb.Helper()

	cluster := createCluster(*flagCluster)
	initOnce.Do(func() {
		createTestKeyspace(tb, cluster, "test_scylla_manager")
	})
	session := createSessionFromCluster(tb, cluster)

	if err := migrate.Migrate(context.Background(), session, SchemaDir()); err != nil {
		tb.Fatal("migrate:", err)
	}
	return session
}

// CreateSessionWithoutMigration clears the database on scylla manager cluster
// and returns a new gocql.Session. This is only useful for testing migrations
// you probably should be using CreateSession instead.
func CreateSessionWithoutMigration(tb testing.TB) *gocql.Session {
	tb.Helper()

	cluster := createCluster(*flagCluster)
	createTestKeyspace(tb, cluster, "test_scylla_manager")
	return createSessionFromCluster(tb, cluster)
}

// CreateManagedClusterSession returns a new gocql.Session to the managed data cluster.
func CreateManagedClusterSession(tb testing.TB) *gocql.Session {
	tb.Helper()

	cluster := createCluster(ManagedClusterHosts()...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *flagUser,
		Password: *flagPassword,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}
	dropAllKeyspaces(tb, session)
	return session
}

func createCluster(hosts ...string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	return cluster
}

func createSessionFromCluster(tb testing.TB, cluster *gocql.ClusterConfig) *gocql.Session {
	tb.Helper()
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
	}`, keyspace, 1))
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
