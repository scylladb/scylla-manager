// Copyright (C) 2017 ScyllaDB

package db

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/ptr"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/multierr"

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

	sessionHosts, err := cluster.GetRPCAddresses(ctx, client, client.Config().Hosts, false)
	if err != nil {
		tb.Log(err)
		if errors.Is(err, cluster.ErrNoRPCAddressesFound) {
			tb.Fatal("no host available: ", err)
		}
	}

	cluster := createCluster(sessionHosts...)
	cluster.Timeout = 5 * time.Minute
	if user == "" && pass == "" {
		user = testconfig.TestDBUsername()
		pass = testconfig.TestDBPassword()
	}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: user,
		Password: pass,
	}
	if os.Getenv("SSL_ENABLED") == "true" {
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

	ExecStmt(tb, session, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace))
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

	RawWriteData(t, session, keyspace, 0, sizeMiB, "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}", true, tables...)
}

// WriteDataSecondClusterSchema creates big_table in the provided keyspace with the size in MiB with replication set for second cluster.
func WriteDataSecondClusterSchema(t *testing.T, session gocqlx.Session, keyspace string, startingID, sizeMiB int, tables ...string) int {
	t.Helper()

	return RawWriteData(t, session, keyspace, startingID, sizeMiB, "{'class': 'NetworkTopologyStrategy', 'dc1': 2}", false, tables...)
}

// RawWriteData creates big_table in the provided keyspace with the size in MiB.
// It returns starting ID for the future calls, so that rows created from different calls to this function does not overlap.
// It's also required to specify keyspace replication and whether table should use compaction.
func RawWriteData(t *testing.T, session gocqlx.Session, keyspace string, startingID, sizeMiB int, replication string, compaction bool, tables ...string) int {
	t.Helper()

	var (
		ksStmt     = "CREATE KEYSPACE IF NOT EXISTS %q WITH replication = %s"
		tStmt      = "CREATE TABLE IF NOT EXISTS %q.%q (id int PRIMARY KEY, data blob) WITH tombstone_gc = {'mode':'repair'}"
		insertStmt = "INSERT INTO %q.%q (id, data) VALUES (?, ?)"
	)
	if !compaction {
		tStmt += " AND compaction = {'enabled': 'false', 'class': 'NullCompactionStrategy'}"
	}

	ExecStmt(t, session, fmt.Sprintf(ksStmt, keyspace, replication))
	if len(tables) == 0 {
		tables = []string{BigTableName}
	}

	bytes := sizeMiB * 1024 * 1024
	rowsCnt := bytes / 4096

	for i := range tables {
		ExecStmt(t, session, fmt.Sprintf(tStmt, keyspace, tables[i]))

		stmt := fmt.Sprintf(insertStmt, keyspace, tables[i])
		q := session.Query(stmt, []string{"id", "data"})

		data := make([]byte, 4096)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}

		for i := range rowsCnt {
			if err := q.Bind(i+startingID, data).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		q.Release()
	}

	return startingID + rowsCnt
}

// CreateMaterializedView is the utility function that executes CQL query creating MV for given keyspace.table.
func CreateMaterializedView(t *testing.T, session gocqlx.Session, keyspace, table, mv string) {
	t.Helper()

	ExecStmt(t, session, fmt.Sprintf("CREATE MATERIALIZED VIEW %q.%q AS SELECT * FROM %q.%q WHERE data IS NOT NULL PRIMARY KEY (id, data)",
		keyspace, mv, keyspace, table))

	WaitForViews(t, session)
}

// CreateSecondaryIndex is the utility function that executes CQL query creating SI for given keyspace.table.
func CreateSecondaryIndex(t *testing.T, session gocqlx.Session, keyspace, table, si string) {
	t.Helper()

	ExecStmt(t, session, fmt.Sprintf("CREATE INDEX %q ON %q.%q (data)", si, keyspace, table))

	WaitForViews(t, session)
}

// FlushTable flushes memtable to sstables. It allows for more precise size calculations.
func FlushTable(t *testing.T, client *scyllaclient.Client, hosts []string, keyspace, table string) {
	t.Helper()

	for _, h := range hosts {
		if err := client.FlushTable(context.Background(), h, keyspace, table); err != nil {
			t.Fatal(err)
		}
	}
}

// WaitForViews returns only when all views in the cluster has been successfully built.
func WaitForViews(t *testing.T, session gocqlx.Session) {
	t.Helper()

	var stats []string
	q := qb.Select("system_distributed.view_build_status").Columns("status").Query(session)
	defer q.Release()
	timer := time.NewTimer(time.Minute)

	for {
		select {
		case <-timer.C:
			t.Fatal("Waiting for view creation timeout")
		default:
		}

		if err := q.Select(&stats); err != nil {
			t.Fatal(err)
		}

		ok := true
		for _, s := range stats {
			if s != "SUCCESS" {
				ok = false
				break
			}
		}

		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

// GetAlternatorCreds creates (if not exists) a CQL role and alternator creds associated with it.
// See https://opensource.docs.scylladb.com/stable/alternator/compatibility.html#authorization for more details.
func GetAlternatorCreds(t *testing.T, s gocqlx.Session, role string) (accessKeyID, secretAccessKey string) {
	t.Helper()

	if role == "" {
		role = testconfig.TestDBUsername()
	}

	ExecStmt(t, s, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %q WITH PASSWORD = '%s' AND SUPERUSER = false AND LOGIN = true", role, role))
	accessKeyID = role

	// Roles table is kept in different keyspaces depending on Scylla version
	rolesKeyspaces := []string{"system", "system_auth"}
	var retErr error
	for _, ks := range rolesKeyspaces {
		q := s.Query(fmt.Sprintf("SELECT salted_hash FROM %s.roles WHERE role = '%s'", ks, role), nil)
		if err := q.Scan(&secretAccessKey); err != nil {
			retErr = multierr.Append(retErr, err)
		} else {
			return accessKeyID, secretAccessKey
		}
	}

	t.Fatal("Couldn't get salted_hash from roles table", retErr)
	return
}

// CreateAlternatorTable creates "alternator_{table}.{table}" table via alternator API.
func CreateAlternatorTable(t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: ptr.String("key"),
			AttributeType: "N",
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: ptr.String("key"),
			KeyType:       "HASH",
		}},
		TableName:   ptr.String(table),
		BillingMode: "PAY_PER_REQUEST",
	}

	_, err := client.CreateTable(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
}

// FillAlternatorTable inserts 100 rows into "alternator_{table}.{table}" table via alternator API.
func FillAlternatorTable(t *testing.T, client *dynamodb.Client, table string, rowCnt int) {
	t.Helper()

	var reqs []types.WriteRequest
	for i := range rowCnt {
		v, err := attributevalue.Marshal(i)
		if err != nil {
			t.Fatal(err)
		}
		reqs = append(reqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"key": v,
				},
			},
		})
	}

	writeIn := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			table: reqs,
		},
	}
	_, err := client.BatchWriteItem(context.Background(), writeIn)
	if err != nil {
		t.Fatal(err)
	}
}

// CreateAlternatorClient returns alternator client.
func CreateAlternatorClient(t *testing.T, client *scyllaclient.Client, host, accessKeyID, secretAccessKey string) *dynamodb.Client {
	t.Helper()

	ni, err := client.NodeInfo(context.Background(), host)
	if err != nil {
		t.Fatal(err)
	}

	awsCfg := aws.Config{
		BaseEndpoint: ptr.String(ni.AlternatorAddr(host)),
		Region:       "scylla",
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}, nil
		}),
	}

	return dynamodb.NewFromConfig(awsCfg)
}

// CreateInterestingAlternatorSchema creates tables with specified names with LSI, GSI, tags and TTL.
func CreateInterestingAlternatorSchema(t *testing.T, client *dynamodb.Client, tables ...string) {
	t.Helper()

	for _, table := range tables {
		_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
			TableName: aws.String(table),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("PK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("SK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("LSI_SK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("GSI_PK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("GSI_SK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("PK"),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String("SK"),
					KeyType:       types.KeyTypeRange,
				},
			},
			LocalSecondaryIndexes: []types.LocalSecondaryIndex{
				{
					IndexName: aws.String(table + "_LSI"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("PK"),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String("LSI_SK"),
							KeyType:       types.KeyTypeRange,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(table + "_GSI"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("GSI_PK"),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String("GSI_SK"),
							KeyType:       types.KeyTypeRange,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
			Tags: []types.Tag{
				{
					Key:   aws.String(table + "_tag"),
					Value: aws.String("1"),
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		if err != nil {
			t.Fatal(err)
		}

		WaitForAlternatorTable(t, client, table)

		_, err = client.UpdateTimeToLive(context.Background(), &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(table),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String(table + "_TTL"),
				Enabled:       aws.Bool(true),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Just to make sure that TTL changes are applied
		time.Sleep(time.Second)
	}
}

// WaitForAlternatorTable waits for alternator tablet o be created.
func WaitForAlternatorTable(t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()

	waiter := dynamodb.NewTableExistsWaiter(client)
	err := waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
}
