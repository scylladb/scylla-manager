package mermaidtest

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
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
	return createSessionFromCluster(createCluster(), tb)
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

func createSessionFromCluster(cluster *gocql.ClusterConfig, tb testing.TB) *gocql.Session {
	initOnce.Do(func() {
		createKeyspace(tb, cluster, "scylla_management")
	})

	cluster.Keyspace = "scylla_management"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
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

	files, err := filepath.Glob("../schema/cql/*.cql")
	if err != nil {
		tb.Fatal(err)
	}
	sort.Strings(files)

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			tb.Fatal(err)
		}
		defer f.Close()

		loadCql(tb, session, f)

		if err := f.Close(); err != nil {
			tb.Fatal(err)
		}
	}
}

func loadCql(tb testing.TB, s *gocql.Session, r io.Reader) {
	var lines []string
	b := bufio.NewReader(r)
	for {
		l, err := b.ReadString('\n')
		if err != nil {
			break
		}
		l = strings.TrimSpace(l)

		// skip empty line
		if len(l) == 0 {
			continue
		}

		// skip comment
		if strings.HasPrefix(l, "--") {
			continue
		}

		lines = append(lines, l)

		// if all, execute
		if strings.HasSuffix(l, ";") {
			mustExec(tb, s, strings.Join(lines, " "))
			lines = nil
		}
	}
}

func mustExec(tb testing.TB, s *gocql.Session, stmt string) {
	if err := s.Query(stmt).RetryPolicy(nil).Exec(); err != nil {
		tb.Fatal("exec failed", stmt, err)
	}
}
