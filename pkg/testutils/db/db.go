// Copyright (C) 2017 ScyllaDB

package db

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/nopmigrate"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/schema"
)

var initOnce sync.Once

// CreateScyllaManagerDBSession recreates the database on scylla manager cluster and returns
// a new gocql.Session.
func CreateScyllaManagerDBSession(tb testing.TB) gocqlx.Session {
	tb.Helper()

	cluster := createCluster(testconfig.ScyllaManagerDBCluster())
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

	cluster := createCluster(testconfig.ScyllaManagerDBCluster())
	createTestKeyspace(tb, cluster, "test_scylla_manager")
	return createSessionFromCluster(tb, cluster)
}

// CreateSessionAndDropAllKeyspaces returns a new gocqlx.Session
// to the managed data cluster and clears all keyspaces.
func CreateSessionAndDropAllKeyspaces(tb testing.TB, client *scyllaclient.Client) gocqlx.Session {
	tb.Helper()
	return CreateManagedClusterSession(tb, true, client, "", "")
}

// CreateSession returns a new gocqlx.Session to the managed data
// cluster without clearing it.
func CreateSession(tb testing.TB, client *scyllaclient.Client) gocqlx.Session {
	tb.Helper()
	return CreateManagedClusterSession(tb, false, client, "", "")
}

// CreateManagedClusterSession return a new gocqlx.Session to the managed cluster.
// It allows to specify cql user and decide if cluster should be cleared.
func CreateManagedClusterSession(tb testing.TB, empty bool, client *scyllaclient.Client, user, pass string) gocqlx.Session {
	tb.Helper()
	ctx := context.Background()

	sessionHosts, err := cluster.GetRPCAddresses(ctx, client, client.Config().Hosts)
	if err != nil {
		tb.Log(err)
		if errors.Is(err, cluster.ErrNoRPCAddressesFound) {
			tb.Fatal("no host available: ", err)
		}
	}

	cluster := createCluster(sessionHosts...)
	if user == "" && pass == "" {
		user = testconfig.TestDBUsername()
		pass = testconfig.TestDBPassword()
	}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: user,
		Password: pass,
	}
	if os.Getenv("SSL_ENABLED") != "" {
		cluster.SslOpts = testconfig.CQLSSLOptions()
		cluster.Port = testconfig.CQLPort()
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
	cluster.Timeout = testconfig.CQLTimeout()
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
	c.Timeout = testconfig.CQLTimeout()
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

// BigTableName is the default name of table used for testing.
const BigTableName = "big_table"

// WriteData creates big_table in the provided keyspace with the size in MiB.
func WriteData(t *testing.T, session gocqlx.Session, keyspace string, sizeMiB int, tables ...string) {
	t.Helper()

	writeData(t, session, keyspace, 0, sizeMiB, "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}", tables...)
}

// WriteDataToSecondCluster creates big_table in the provided keyspace with the size in MiB with replication set for second cluster.
func WriteDataToSecondCluster(t *testing.T, session gocqlx.Session, keyspace string, startingID, sizeMiB int, tables ...string) int {
	t.Helper()

	return writeData(t, session, keyspace, startingID, sizeMiB, "{'class': 'NetworkTopologyStrategy', 'dc1': 1}", tables...)
}

// WriteData creates big_table in the provided keyspace with the size in MiB.
// It returns starting ID for the future calls, so that rows created from different calls to this function does not overlap.
func writeData(t *testing.T, session gocqlx.Session, keyspace string, startingID, sizeMiB int, replication string, tables ...string) int {
	t.Helper()

	ExecStmt(t, session, "CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication = "+replication)

	if len(tables) == 0 {
		tables = []string{BigTableName}
	}

	bytes := sizeMiB * 1024 * 1024
	rowsCnt := bytes / 4096

	for i := range tables {
		ExecStmt(t, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, data blob)", keyspace, tables[i]))
		stmt := fmt.Sprintf("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace, tables[i])
		q := session.Query(stmt, []string{"id", "data"})
		defer q.Release()

		data := make([]byte, 4096)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < rowsCnt; i++ {
			if err := q.Bind(i+startingID, data).Exec(); err != nil {
				t.Fatal(err)
			}
		}
	}

	return startingID + rowsCnt
}
