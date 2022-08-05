// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/nopmigrate"
	"github.com/scylladb/scylla-manager/v3/schema"
)

var (
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples of scylla manager db hosts")

	flagTimeout      = flag.Duration("gocql.timeout", 10*time.Second, "sets the connection `timeout` for all operations")
	flagPort         = flag.Int("gocql.port", 9042, "sets the port used to connect to the database cluster")
	flagUser         = flag.String("user", "", "CQL user")
	flagPassword     = flag.String("password", "", "CQL password")
	flagCAFile       = flag.String("ssl-ca-file", "", "Certificate Authority file")
	flagUserCertFile = flag.String("ssl-cert-file", "", "User SSL certificate file")
	flagUserKeyFile  = flag.String("ssl-key-file", "", "User SSL key file")
	flagValidate     = flag.Bool("ssl-validate", false, "Enable host verification")

	flagManagedCluster = flag.String("managed-cluster", "127.0.0.1", "a comma-separated list of host:port tuples of data cluster hosts")
)

// ManagedClusterHosts specifies addresses of nodes in a test cluster.
func ManagedClusterHosts() []string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return strings.Split(*flagManagedCluster, ",")
}

// ManagedClusterHost returns ManagedClusterHosts()[0].
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

var initOnce sync.Once

// CreateScyllaManagerDBSession recreates the database on scylla manager cluster and returns
// a new gocql.Session.
func CreateScyllaManagerDBSession(tb testing.TB) gocqlx.Session {
	tb.Helper()

	cluster := createCluster(*flagCluster)
	initOnce.Do(func() {
		createTestKeyspace(tb, cluster, "test_scylla_manager")
	})
	session := createSessionFromCluster(tb, cluster)

	migrate.Callback = nopmigrate.Callback
	if err := migrate.FromFS(context.Background(), session, schema.Files); err != nil {
		tb.Fatal("migrate:", err)
	}
	return session
}

// CreateSessionWithoutMigration clears the database on scylla manager cluster
// and returns a new gocqlx.Session. This is only useful for testing migrations
// you probably should be using CreateScyllaManagerDBSession instead.
func CreateSessionWithoutMigration(tb testing.TB) gocqlx.Session {
	tb.Helper()

	cluster := createCluster(*flagCluster)
	createTestKeyspace(tb, cluster, "test_scylla_manager")
	return createSessionFromCluster(tb, cluster)
}

// CreateSessionAndDropAllKeyspaces returns a new gocqlx.Session
// to the managed data cluster and clears all keyspaces.
func CreateSessionAndDropAllKeyspaces(tb testing.TB, hosts []string) gocqlx.Session {
	tb.Helper()
	return createManagedClusterSession(tb, true, hosts)
}

// CreateSession returns a new gocqlx.Session to the managed data
// cluster without clearing it.
func CreateSession(tb testing.TB, hosts []string) gocqlx.Session {
	tb.Helper()
	return createManagedClusterSession(tb, false, hosts)
}

func createManagedClusterSession(tb testing.TB, empty bool, hosts []string) gocqlx.Session {
	tb.Helper()

	cluster := createCluster(hosts...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *flagUser,
		Password: *flagPassword,
	}
	if os.Getenv("SSL_ENABLED") != "" {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 *flagCAFile,
			CertPath:               *flagUserCertFile,
			KeyPath:                *flagUserKeyFile,
			EnableHostVerification: *flagValidate,
		}
		cluster.Port = *flagPort
	}

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		tb.Fatal("createSession:", err)
	}
	if empty {
		dropAllKeyspaces(tb, session)
	}
	return session
}

func createCluster(hosts ...string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	return cluster
}

func createSessionFromCluster(tb testing.TB, cluster *gocql.ClusterConfig) gocqlx.Session {
	tb.Helper()
	cluster.Keyspace = "test_scylla_manager"
	cluster.Timeout = *flagTimeout
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		tb.Fatal("createSession:", err)
	}

	return session
}

func createTestKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	tb.Helper()

	c := *cluster
	c.Keyspace = "system"
	c.Timeout = *flagTimeout
	session, err := gocqlx.WrapSession(c.CreateSession())
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

func dropAllKeyspaces(tb testing.TB, session gocqlx.Session) {
	tb.Helper()

	q := qb.Select("system_schema.keyspaces").Columns("keyspace_name").Query(session)
	defer q.Release()

	var all []string
	if err := q.Select(&all); err != nil {
		tb.Fatal(err)
	}

	for _, k := range all {
		if !strings.HasPrefix(k, "system") {
			dropKeyspace(tb, session, k)
		}
	}
}

func dropKeyspace(tb testing.TB, session gocqlx.Session, keyspace string) {
	tb.Helper()

	ExecStmt(tb, session, "DROP KEYSPACE IF EXISTS "+keyspace)
}

// ExecStmt executes given statement.
func ExecStmt(tb testing.TB, session gocqlx.Session, stmt string) {
	tb.Helper()

	if err := session.ExecStmt(stmt); err != nil {
		tb.Fatal("exec failed", stmt, err)
	}
}

const bigTableName = "big_table"

// WriteData creates big_table in the provided keyspace with the size in MiB.
func WriteData(t *testing.T, session gocqlx.Session, keyspace string, sizeMiB int, tables ...string) {
	t.Helper()

	writeData(t, session, keyspace, sizeMiB, "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}", tables...)
}

// WriteData creates big_table in the provided keyspace with the size in MiB.
func writeData(t *testing.T, session gocqlx.Session, keyspace string, sizeMiB int, replication string, tables ...string) {
	t.Helper()

	ExecStmt(t, session, "CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication = "+replication)

	if len(tables) == 0 {
		tables = []string{bigTableName}
	}

	for i := range tables {
		ExecStmt(t, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, data blob)", keyspace, tables[i]))
		stmt := fmt.Sprintf("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace, tables[i])
		q := session.Query(stmt, []string{"id", "data"})
		defer q.Release()

		bytes := sizeMiB * 1024 * 1024
		data := make([]byte, 4096)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < bytes/4096; i++ {
			if err := q.Bind(i, data).Exec(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
