// Copyright (C) 2017 ScyllaDB
//
// Creating this package as a testing tool to aid in creating tables with
// certain amount of sstable file size. Primary usage is creating tables that
// would take while to backup.

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	flagCluster      = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto        = flag.Int("proto", 0, "protcol version")
	flagCQL          = flag.String("cql", "3.0.0", "CQL version")
	flagKeyspace     = flag.String("keyspace", "", "name of the keyspace to create table in")
	flagDropKeyspace = flag.Bool("drop", false, "drop keyspace if it already exists")
	flagTable        = flag.String("table", "", "name of the table to create")
	flagTableSize    = flag.Int64("size", 0, "size of the table to create in MB")
	flagRF           = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry        = flag.Int("retries", 5, "number of times to retry queries")
	flagCompressTest = flag.String("compressor", "", "compressor to use")
	flagTimeout      = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	clusterHosts []string
)

func init() {
	flag.Parse()
	clusterHosts = strings.Split(*flagCluster, ",")
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func createCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &gocql.SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	return cluster
}

func createTable(cluster *gocql.ClusterConfig, keyspace, table string) {
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	if *flagDropKeyspace {
		err = ExecStmt(session, `DROP KEYSPACE IF EXISTS `+keyspace)
		if err != nil {
			log.Fatalf("unable to drop keyspace: %v", err)
		}

		err = ExecStmt(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))

		if err != nil {
			log.Fatalf("unable to create keyspace: %v", err)
		}
	}

	if err := ExecStmt(session, fmt.Sprintf("CREATE TABLE %s.%s (id int PRIMARY KEY, data blob)", keyspace, table)); err != nil {
		log.Fatal(err)
	}

	mbSize := *flagTableSize
	for i := int64(0); i <= mbSize/10; i++ {
		var size int64 = 10
		if i == mbSize/10 {
			size = mbSize % 10
			if size == 0 {
				continue
			}
		}
		data := make([]byte, size*1024*1024)
		if _, err := rand.Read(data); err != nil {
			log.Fatal(err)
		}
		query := fmt.Sprintf("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace, table)
		q := session.Query(query, i, data)
		if err := q.Exec(); err != nil {
			log.Fatal(err)
		}
		q.Release()
	}
}

// ExecStmt executes a statement string.
func ExecStmt(s *gocql.Session, stmt string) error {
	q := s.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	return q.Exec()
}

func main() {
	cluster := createCluster()
	createTable(cluster, *flagKeyspace, *flagTable)
	fmt.Printf("Created table with %d MiB of data\n", *flagTableSize)
}
