// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	schematable "github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestRestoreTablesUserIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("Auth restore is not supported in Scylla 6.0. It requires core side support that is aimed at 6.1 release")
	}

	user := randomizedName("user_")
	pass := randomizedName("pass_")
	Printf("Create user (%s/%s) to be backed-up", user, pass)
	createUser(t, h.srcCluster.rootSession, user, pass)
	ExecStmt(t, h.srcCluster.rootSession, "GRANT CREATE ON ALL KEYSPACES TO "+user)

	Print("Run backup")
	loc := testLocation("user", "")
	S3InitBucket(t, loc.Path)
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	h.runRestore(t, defaultTestProperties(loc, tag, true))

	Print("Log in via restored user and check permissions")
	userSession := CreateManagedClusterSession(t, false, h.dstCluster.Client, user, pass)
	newKs := randomizedName("ks_")
	ExecStmt(t, userSession, fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}", newKs))
}

func TestRestoreTablesNoReplicationIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("no_rep_ks_")
	tab := randomizedName("tab_")
	Printf("Create non replicated %s.%s in both cluster", ks, tab)
	ksStmt := fmt.Sprintf("CREATE KEYSPACE %q WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ks)
	tabStmt := fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data int)", ks, tab)
	ExecStmt(t, h.srcCluster.rootSession, ksStmt)
	ExecStmt(t, h.srcCluster.rootSession, tabStmt)
	ExecStmt(t, h.dstCluster.rootSession, ksStmt)
	ExecStmt(t, h.dstCluster.rootSession, tabStmt)

	Print("Fill created table")
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{"id", "data"})
	defer q.Release()
	for i := 0; i < 100; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := testLocation("no-replication", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = ksFilter
	h.runRestore(t, props)

	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreSchemaRoundtripIntegration(t *testing.T) {
	// Test scenario for both CQL and alternator schema:
	// - create schema on src cluster
	// - back up src cluster
	// - restore src cluster schema to dst cluster
	// - drop src cluster schema
	// - back up dst cluster
	// - restore dst cluster schema to src cluster
	// - validates that schema was correct at all stages
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())
	hRev := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	if !CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("This test assumes that schema is backed up and restored via DESCRIBE SCHEMA WITH INTERNALS")
	}

	Print("Prepare CQL schema with non-default options")
	cqlKs := randomizedName("roundtrip_")
	cqlTab := randomizedName("tab_")
	ksOpt := "durable_writes = false"
	tabOpt := "compaction = {'class': 'NullCompactionStrategy', 'enabled': 'false'}"
	objWithOpt := map[string]string{
		cqlKs:  ksOpt,
		cqlTab: tabOpt,
	}
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} AND %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, cqlKs, 2, ksOpt))
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data blob) WITH %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, cqlKs, cqlTab, tabOpt))

	Print("Prepare alternator schema")
	altTab1 := "roundtrip_1_" + AlternatorProblematicTableChars
	altTab2 := "roundtrip_2_" + AlternatorProblematicTableChars
	altGSI1 := "gsi_1_" + AlternatorProblematicTableChars
	altGSI2 := "gsi_2_" + AlternatorProblematicTableChars
	altTag := "tag"
	altTTLAttr := "ttl_attr"
	CreateAlternatorTable(t, h.srcCluster.altClient, 2, altTab1, altTab2)
	CreateAlternatorGSI(t, h.srcCluster.altClient, altTab1, altGSI1, altGSI2)
	CreateAlternatorGSI(t, h.srcCluster.altClient, altTab2, altGSI1, altGSI2)
	TagAlternatorTable(t, h.srcCluster.altClient, altTab1, altTag)
	TagAlternatorTable(t, h.srcCluster.altClient, altTab2, altTag)
	UpdateAlternatorTableTTL(t, h.srcCluster.altClient, altTab1, altTTLAttr, true)
	UpdateAlternatorTableTTL(t, h.srcCluster.altClient, altTab2, altTTLAttr, true)

	Print("Save src CQL schema")
	srcCQLSchema, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get src CQL schema"))
	}

	Print("Save src alternator schema")
	srcAltSchema, err := backup.GetAlternatorSchema(context.Background(), h.srcCluster.altClient)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get src alternator schema"))
	}

	Print("Run src backup")
	loc := testLocation("schema-roundtrip", "")
	S3InitBucket(t, loc.Path)
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Drop backed-up src cluster CQL schema")
	ExecStmt(t, h.srcCluster.rootSession, "DROP KEYSPACE "+cqlKs)

	Print("Drop backed-up src cluster alternator schema")
	_, err = h.srcCluster.altClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(altTab1)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.srcCluster.altClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(altTab2)})
	if err != nil {
		t.Fatal(err)
	}

	Print("Run restore of src backup on dst cluster")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, defaultTestProperties(loc, tag, false))

	Print("Save dst CQL schema from src backup")
	dstCQLSchemaSrcBackup, err := query.DescribeSchemaWithInternals(h.dstCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get dst CQL schema from src backup"))
	}

	Print("Save dst alternator schema from src backup")
	dstAltSchemaSrcBackup, err := backup.GetAlternatorSchema(context.Background(), h.dstCluster.altClient)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get dst alternator schema from src backup"))
	}

	Print("Run dst backup")
	tag = hRev.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Run restore of dst backup on src cluster")
	grantRestoreSchemaPermissions(t, hRev.dstCluster.rootSession, hRev.dstUser)
	hRev.runRestore(t, defaultTestProperties(loc, tag, false))

	Print("Save src CQL schema from dst backup")
	srcCQLSchemaDstBackup, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get src CQL schema from dst backup"))
	}

	Print("Save src alternator schema output from dst backup")
	srcAltSchemaDstBackup, err := backup.GetAlternatorSchema(context.Background(), h.srcCluster.altClient)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get src alternator schema from dst backup"))
	}

	Print("Validate that CQL schema contains objects with options")
	var (
		m1 = map[query.DescribedSchemaRow]struct{}{}
		m2 = map[query.DescribedSchemaRow]struct{}{}
		m3 = map[query.DescribedSchemaRow]struct{}{}
	)
	for _, row := range srcCQLSchema {
		// Scylla 6.3 added roles and service levels to the output of
		// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
		// Those entities do not live in any particular keyspace, so that's how we identify them.
		// We are skipping them until we properly support their restoration.
		if row.Keyspace == "" {
			continue
		}
		// Don't validate alternator schema CQL statements
		if strings.HasPrefix(row.Keyspace, "\"alternator_") {
			continue
		}
		m1[row] = struct{}{}
		if opt, ok := objWithOpt[row.Name]; ok {
			if !strings.Contains(row.CQLStmt, opt) {
				t.Fatalf("Object: %v, with cql: %v, does not contain option: %v", row.Name, row.CQLStmt, opt)
			}
			delete(objWithOpt, row.Name)
		}
	}
	if len(objWithOpt) > 0 {
		t.Fatalf("Src CQL schema: %v, is missing created objects: %v", m1, objWithOpt)
	}
	for _, row := range dstCQLSchemaSrcBackup {
		if row.Keyspace != "" && !strings.HasPrefix(row.Keyspace, "\"alternator_") {
			m2[row] = struct{}{}
		}
	}
	for _, row := range srcCQLSchemaDstBackup {
		if row.Keyspace != "" && !strings.HasPrefix(row.Keyspace, "\"alternator_") {
			m3[row] = struct{}{}
		}
	}
	Print("Validate that all CQL schemas are the same")
	if !maputil.Equal(m1, m2) {
		t.Fatalf("Src CQL schema:\n%v\nDst CQL schema from src backup:\n%v\nAre not equal", m1, m2)
	}
	if !maputil.Equal(m1, m3) {
		t.Fatalf("Src CQL schema:\n%v\nDst CQL schema from dst backup:\n%v\nAre not equal", m1, m3)
	}

	Print("Validate alternator schema")
	sanitizeAltSchema := func(schema backupspec.AlternatorSchema) {
		// Sort so that we can compare raw json encoding later
		slices.SortFunc(schema.Tables, func(a, b backupspec.AlternatorTableSchema) int {
			return cmp.Compare(*a.Describe.TableName, *b.Describe.TableName)
		})
		for i := range schema.Tables {
			// Set TTL to nil as we don't restore it
			schema.Tables[i].TTL = nil
			// Set times to nil as they are expected to differ
			schema.Tables[i].Describe.CreationDateTime = nil
			schema.Tables[i].Describe.BillingModeSummary.LastUpdateToPayPerRequestDateTime = nil
			schema.Tables[i].Describe.ProvisionedThroughput.LastDecreaseDateTime = nil
			schema.Tables[i].Describe.ProvisionedThroughput.LastIncreaseDateTime = nil
			// Set table ID to ni as it is expected to differ
			schema.Tables[i].Describe.TableId = nil
			slices.SortFunc(schema.Tables[i].Describe.GlobalSecondaryIndexes, func(a, b types.GlobalSecondaryIndexDescription) int {
				return cmp.Compare(*a.IndexName, *b.IndexName)
			})
			slices.SortFunc(schema.Tables[i].Describe.LocalSecondaryIndexes, func(a, b types.LocalSecondaryIndexDescription) int {
				return cmp.Compare(*a.IndexName, *b.IndexName)
			})
		}
	}
	sanitizeAltSchema(srcAltSchema)
	jsonSrcAltSchema, err := json.Marshal(srcAltSchema)
	if err != nil {
		t.Fatal(err)
	}
	sanitizeAltSchema(dstAltSchemaSrcBackup)
	jsonDstAltSchemaSrcBackup, err := json.Marshal(dstAltSchemaSrcBackup)
	if err != nil {
		t.Fatal(err)
	}
	sanitizeAltSchema(srcAltSchemaDstBackup)
	jsonSrcAltSchemaDstBackup, err := json.Marshal(srcAltSchemaDstBackup)
	if err != nil {
		t.Fatal(err)
	}
	if string(jsonSrcAltSchema) != string(jsonDstAltSchemaSrcBackup) {
		t.Fatalf("Src alternator schema:\n%v\nDst alternator schema from src backup:\n%v\nAre not equal", string(jsonSrcAltSchema), string(jsonDstAltSchemaSrcBackup))
	}
	if string(jsonSrcAltSchema) != string(jsonSrcAltSchemaDstBackup) {
		t.Fatalf("Src alternator schema:\n%v\nDst alternator schema from dst backup:\n%v\nAre not equal", string(jsonSrcAltSchema), string(jsonSrcAltSchemaDstBackup))
	}
}

func TestRestoreSchemaDropAddColumnIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if !CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("This test is the reason why SM needs to restore schema by DESCRIBE SCHEMA WITH INTERNALS")
	}

	ks := randomizedName("drop_add_")
	tab := randomizedName("tab_")

	Print(fmt.Sprintf("Create %q.%q with disabled compaction", ks, tab))
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int) WITH compaction = {'class': 'NullCompactionStrategy', 'enabled': 'false'}"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill created table")
	rowCnt := 100
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{"id", "data"})
	defer q.Release()
	for i := 0; i < rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Drop and add column")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf("ALTER TABLE %q.%q DROP data", ks, tab))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf("ALTER TABLE %q.%q ADD data int", ks, tab))

	Print("Fill altered table again")
	for i := rowCnt; i < 2*rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := testLocation("drop-add", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))

	Print("Run restore schema")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, defaultTestProperties(loc, tag, false))

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = ksFilter
	h.runRestore(t, props)

	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreTablesVnodeToTabletsIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ni, err := h.dstCluster.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(errors.Wrap(err, "get any node info"))
	}
	if !ni.EnableTablets {
		t.Skip("This test assumes that tablets are supported")
	}

	ks := randomizedName("vnode_to_tablet_")
	tab := randomizedName("tab_")
	c1 := "id"
	c2 := "data"

	Print(fmt.Sprintf("Create %q.%q with vnode replication", ks, tab))
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} AND tablets = {'enabled': '%v'}"
	tabStmt := "CREATE TABLE %q.%q (%s int PRIMARY KEY, %s int)"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2, false))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, c1, c2))

	Print("Fill created table")
	rowCnt := 100
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{c1, c2})
	defer q.Release()
	for i := 0; i < rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := testLocation("vnode-to-tablets", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))

	Print("Manually recreate tablet schema")
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 3, true))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, c1, c2))

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = ksFilter
	h.runRestore(t, props)

	validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab, c1, c2)
}

func TestRestoreTablesPausedIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	// Setup:
	// ks1: tab, mv, si
	// ks2: tab1, tab2, mv1

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks1 := randomizedName("paused_1_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks1, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks1, 1))
	ks2 := randomizedName("paused_2_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks2, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks2, 1))

	Print("Table setup")
	tab := randomizedName("tab_")
	createTable(t, h.srcCluster.rootSession, ks1, tab)
	createTable(t, h.dstCluster.rootSession, ks1, tab)
	tab1 := randomizedName("tab_1_")
	createTable(t, h.srcCluster.rootSession, ks2, tab1)
	createTable(t, h.dstCluster.rootSession, ks2, tab1)
	tab2 := randomizedName("tab_2_")
	createTable(t, h.srcCluster.rootSession, ks2, tab2)
	createTable(t, h.dstCluster.rootSession, ks2, tab2)

	units := []backup.Unit{
		{
			Keyspace:  ks1,
			Tables:    []string{tab},
			AllTables: true,
		},
		{
			Keyspace:  ks2,
			Tables:    []string{tab1, tab2},
			AllTables: true,
		},
	}

	// It's not possible to create views on tablet keyspaces
	rd := scyllaclient.NewRingDescriber(context.Background(), h.srcCluster.Client)
	if !rd.IsTabletKeyspace(ks1) {
		Print("View setup (ks1)")
		mv := randomizedName("mv_")
		CreateMaterializedView(t, h.srcCluster.rootSession, ks1, tab, mv)
		CreateMaterializedView(t, h.dstCluster.rootSession, ks1, tab, mv)
		si := randomizedName("si_")
		CreateSecondaryIndex(t, h.srcCluster.rootSession, ks1, tab, si)
		CreateSecondaryIndex(t, h.dstCluster.rootSession, ks1, tab, si)
		units[0].Tables = append(units[0].Tables, mv, si+"_index")
	}
	if !rd.IsTabletKeyspace(ks2) {
		Print("View setup (ks2)")
		mv1 := randomizedName("mv_1_")
		CreateMaterializedView(t, h.srcCluster.rootSession, ks2, tab1, mv1)
		CreateMaterializedView(t, h.dstCluster.rootSession, ks2, tab1, mv1)
		units[1].Tables = append(units[1].Tables, mv1)
	}

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks1, tab)
	fillTable(t, h.srcCluster.rootSession, 100, ks2, tab1, tab2)

	Print("Run backup")
	loc := testLocation("paused", "")
	S3InitBucket(t, loc.Path)

	// Starting from SM 3.3.1, SM does not allow to back up views,
	// but backed up views should still be tested as older backups might
	// contain them. That's why here we manually force backup target
	// to contain the views.
	ctx := context.Background()
	h.srcCluster.TaskID = uuid.NewTime()
	h.srcCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(defaultTestBackupProperties(loc, ""))
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	target, err := h.srcBackupSvc.GetTarget(ctx, h.srcCluster.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "generate target"))
	}
	target.Units = units

	err = h.srcBackupSvc.Backup(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID, target)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	pr, err := h.srcBackupSvc.GetProgress(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	tag := pr.SnapshotTag

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks1, ks2}, h.dstUser)
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = []string{ks1, ks2}
	err = runPausedRestore(t, func(ctx context.Context) error {
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			return err
		}
		return h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
	}, 5*time.Second, 20*time.Second, 35*time.Second, 20*time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	for _, u := range units {
		for _, tb := range u.Tables {
			validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, u.Keyspace, tb, "id", "data")
		}
	}
}

func TestRestoreTablesPreparationIntegration(t *testing.T) {
	// Scenario - setup corresponds to things like tombstone_gc mode or compaction being enabled:
	// Run restore - hang on restore data stage
	// Validate setup
	// Pause restore
	// Validate setup
	// Resume restore - hang on restore data stage
	// Validate setup
	// Resume restore - wait for success
	// Validate setup
	// Validate restore success

	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	ni, err := h.dstCluster.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	nativeBackupSupport, err := version.CheckConstraint(ni.ScyllaVersion, ">= 2025.2")
	if err != nil {
		t.Fatal(err)
	}
	nativeBackup := nativeBackupSupport && !IsIPV6Network()
	// Note that this is just a test check - it does not reflect ni.SupportsNativeRestoreAPI().
	nativeRestoreSupport, err := version.CheckConstraint(ni.ScyllaVersion, ">= 2025.3")
	if err != nil {
		t.Fatal(err)
	}
	nativeRestore := nativeRestoreSupport && !IsIPV6Network()

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("prep_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int) WITH tombstone_gc = {'mode': 'repair'}"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	setDstInterceptor := func(interceptor httpx.RoundTripperFunc) {
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if interceptor != nil {
				return interceptor.RoundTrip(req)
			}
			return nil, nil
		}))
	}
	setDstInterceptor(nil)

	validateState := func(ch clusterHelper, tombstone string, compaction bool, transfers int, rateLimit int, cpus []int64, ttl int64) {
		// Validate tombstone_gc mode
		if got := tombstoneGCMode(t, ch.rootSession, ks, tab); tombstone != got {
			t.Errorf("expected tombstone_gc=%s, got %s", tombstone, got)
		}
		// Validate user task TTL
		if nativeBackupSupport {
			for _, host := range ch.Client.Config().Hosts {
				got, err := ch.Client.ScyllaGetUserTaskTTL(context.Background(), host)
				if err != nil {
					t.Errorf("check user task ttl on host %s: %s", host, err)
				}
				if ttl != got {
					t.Errorf("Expected user_task_ttl=%d, got=%d on host %s", ttl, got, host)
				}
			}
		}
		// Validate compaction
		for _, host := range ch.Client.Config().Hosts {
			enabled, err := ch.Client.IsAutoCompactionEnabled(context.Background(), host, ks, tab)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check compaction on host %s", host))
			}
			if compaction != enabled {
				t.Errorf("expected compaction enabled=%v, got=%v on host %s", compaction, enabled, host)
			}
		}
		// Validate cpu pinning
		for _, host := range ch.Client.Config().Hosts {
			got, err := ch.Client.GetPinnedCPU(context.Background(), host)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check cpus on host %s", host))
			}
			slices.Sort(cpus)
			slices.Sort(got)
			if !slices.Equal(cpus, got) {
				t.Errorf("expected cpus=%v, got=%v on host %s", cpus, got, host)
			}
		}
	}

	pinnedCPU := []int64{0} // Taken from scylla-manager-agent.yaml used for testing
	unpinnedCPU := make([]int64, runtime.NumCPU())
	for i := range unpinnedCPU {
		unpinnedCPU[i] = int64(i)
	}

	shardCnt, err := h.dstCluster.Client.ShardCount(context.Background(), ManagedClusterHost())
	if err != nil {
		t.Fatal(err)
	}
	transfers0 := 2 * int(shardCnt)

	setTransfersAndRateLimitAndPinnedCPU := func(ch clusterHelper, transfers int, rateLimit int, pin bool, ttl int64) {
		for _, host := range ch.Client.Config().Hosts {
			if nativeBackupSupport {
				if err := ch.Client.ScyllaSetUserTaskTTL(context.Background(), host, ttl); err != nil {
					t.Fatalf("Set user task ttl on host %s: %s", host, err)
				}
			}
			err := ch.Client.RcloneSetTransfers(context.Background(), host, transfers)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "set transfers on host %s", host))
			}
			err = ch.Client.RcloneSetBandwidthLimit(context.Background(), host, rateLimit)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "set rate limit on host %s", host))
			}
			if pin {
				err = ch.Client.PinCPU(context.Background(), host)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "pin CPUs on host %s", host))
				}
			} else {
				err = ch.Client.UnpinFromCPU(context.Background(), host)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "unpin CPUs on host %s", host))
				}
			}
		}
	}

	Print("Set initial config")
	setTransfersAndRateLimitAndPinnedCPU(h.srcCluster, 10, 99, true, 1)
	setTransfersAndRateLimitAndPinnedCPU(h.dstCluster, 10, 99, true, 1)

	Print("Validate state before backup")
	validateState(h.srcCluster, "repair", true, 10, 99, pinnedCPU, 1)

	Print("Run backup")
	loc := testLocation("preparation", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(loc, ks)
	backupProps["transfers"] = 3
	backupProps["rate_limit"] = []string{"88"}
	backupProps["method"] = backup.MethodAuto
	tag := h.runBackup(t, backupProps)

	Print("Validate state after backup")
	if nativeBackup {
		validateState(h.srcCluster, "repair", true, 10, 99, pinnedCPU, 1)
	} else {
		validateState(h.srcCluster, "repair", true, 3, 88, pinnedCPU, 1)
	}

	runRestore := func(ctx context.Context, finishedRestore chan error) {
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.RunID = uuid.NewTime()
		props := defaultTestProperties(loc, tag, true)
		props["keyspace"] = ksFilter
		props["transfers"] = 0
		props["rate_limit"] = []string{"0"}
		props["unpin_agent_cpu"] = true
		props["method"] = restore.MethodAuto
		if nativeRestore {
			props["method"] = restore.MethodNative
		}
		rawProps, err := json.Marshal(props)
		if err != nil {
			finishedRestore <- err
			return
		}
		finishedRestore <- h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
	}

	makeLASHang := func(reachedDataStageChan, hangLAS chan struct{}) {
		cnt := atomic.Int64{}
		cnt.Add(int64(len(h.dstCluster.Client.Config().Hosts)))
		setDstInterceptor(func(req *http.Request) (*http.Response, error) {
			if isLasOrRestoreEndpoint(req.URL.Path) {
				if curr := cnt.Add(-1); curr == 0 {
					Print("Reached data stage")
					close(reachedDataStageChan)
				}
				Print("Wait for LAS to stop hanging")
				<-hangLAS
			}
			return nil, nil
		})
	}

	var (
		reachedDataStageChan = make(chan struct{})
		hangLAS              = make(chan struct{})
	)
	Print("Make LAS hang")
	makeLASHang(reachedDataStageChan, hangLAS)

	Print("Validate state before restore")
	validateState(h.dstCluster, "repair", true, 10, 99, pinnedCPU, 1)

	Print("Run restore")
	finishedRestore := make(chan error)
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	go runRestore(restoreCtx, finishedRestore)

	Print("Wait for data stage")
	select {
	case <-reachedDataStageChan:
	case err := <-finishedRestore:
		t.Fatalf("Restore finished before reaching data stage with: %s", err)
	}

	Print("Validate state during restore data")
	if nativeRestore {
		validateState(h.dstCluster, "disabled", false, 10, 99, unpinnedCPU, scyllaclient.ManagerTaskTTLSeconds)
	} else {
		validateState(h.dstCluster, "disabled", false, transfers0, 0, unpinnedCPU, 1)
	}

	Print("Pause restore")
	restoreCancel()

	Print("Release LAS")
	close(hangLAS)

	Print("Wait for restore")
	err = <-finishedRestore
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected restore to be paused, got: %s", err)
	}

	Print("Validate state during pause")
	if nativeRestore {
		validateState(h.dstCluster, "disabled", true, 10, 99, pinnedCPU, 1)
	} else {
		validateState(h.dstCluster, "disabled", true, transfers0, 0, pinnedCPU, 1)
	}

	Print("Change transfers and rate limit during pause")
	setTransfersAndRateLimitAndPinnedCPU(h.dstCluster, 9, 55, false, 2)

	reachedDataStageChan = make(chan struct{})
	hangLAS = make(chan struct{})
	Print("Make LAS hang after pause")
	makeLASHang(reachedDataStageChan, hangLAS)

	Print("Run restore after pause")
	finishedRestore = make(chan error)
	go runRestore(context.Background(), finishedRestore)

	Print("Wait for data stage")
	select {
	case <-reachedDataStageChan:
	case err := <-finishedRestore:
		t.Fatalf("Restore finished before reaching data stage with: %s", err)
	}

	Print("Validate state during restore data after pause")
	if nativeRestore {
		validateState(h.dstCluster, "disabled", false, 9, 55, unpinnedCPU, scyllaclient.ManagerTaskTTLSeconds)
	} else {
		validateState(h.dstCluster, "disabled", false, transfers0, 0, unpinnedCPU, 2)
	}

	Print("Release LAS")
	close(hangLAS)

	Print("Wait for restore")
	err = <-finishedRestore
	if err != nil {
		t.Fatalf("Expected restore to success, got: %s", err)
	}

	Print("Validate state after restore success")
	if nativeRestore {
		validateState(h.dstCluster, "repair", true, 9, 55, pinnedCPU, 2)
	} else {
		validateState(h.dstCluster, "repair", true, transfers0, 0, pinnedCPU, 2)
	}

	Print("Validate table contents")
	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreTablesBatchRetryIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("batch_retry_1_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab1 := randomizedName("tab_1_")
	tab2 := randomizedName("tab_2_")
	tab3 := randomizedName("tab_3_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab1))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab2))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab3))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab3))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab1, tab2, tab3)

	Print("Run backup")
	loc := testLocation("batch-retry", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(loc, ks)
	backupProps["batch_size"] = 100
	tag := h.runBackup(t, backupProps)

	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = ksFilter

	t.Run("batch retry finished with success", func(t *testing.T) {
		Print("Inject errors to some download and las calls")
		counter := atomic.Int64{}
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			// For this setup, we have 6 remote sstable dirs and 6 workers.
			// We inject 5 errors during LAS or Scylla Restore API.
			// This means that only a single node will be restoring at the end.
			// Huge batch size and 5 errors guarantee total 11 calls to LAS or Scylla API.
			// The last failed call (cnt=10) waits a bit so that we test
			// that batch dispatcher correctly reuses and releases nodes waiting
			// for failed sstables to come back to the batch dispatcher.
			if isLasOrRestoreEndpoint(req.URL.Path) {
				cnt := counter.Add(1)
				switch cnt {
				case 10:
					time.Sleep(15 * time.Second)
					return nil, errors.New("fake error")
				case 1, 3, 5, 8:
					return nil, errors.New("fake error")
				}
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.runRestore(t, props)

		Print("Validate success")
		if cnt := counter.Add(0); cnt < 11 {
			t.Fatalf("Expected at least 9 calls to LAS, got %d", cnt)
		}
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab1, "id", "data")
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab2, "id", "data")
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab3, "id", "data")
	})

	t.Run("restore with injected failures only", func(t *testing.T) {
		Print("Inject errors to all download and las calls")
		reachedDataStage := atomic.Bool{}
		reachedDataStageChan := make(chan struct{})
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if isDownloadOrRestoreEndpoint(req.URL.Path) {
				if reachedDataStage.CompareAndSwap(false, true) {
					close(reachedDataStageChan)
				}
				return nil, errors.New("fake error")
			}
			if isLasOrRestoreEndpoint(req.URL.Path) {
				return nil, errors.New("fake error")
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.TaskID = uuid.NewTime()
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			t.Fatal(errors.Wrap(err, "marshal properties"))
		}
		res := make(chan error)
		go func() {
			res <- h.dstRestoreSvc.Restore(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
		}()

		Print("Wait for data stage")
		select {
		case <-reachedDataStageChan:
		case err := <-res:
			t.Fatalf("Restore finished before reaching data stage with: %s", err)
		}

		Print("Validate restore failure and that it does not hang")
		select {
		case err := <-res:
			if err == nil {
				t.Fatalf("Expected restore to end with error")
			}
		case <-time.NewTimer(time.Minute).C:
			t.Fatal("Restore hanged")
		}
	})

	t.Run("paused restore with slow calls to download and las", func(t *testing.T) {
		Print("Make download and las calls slow")
		reachedDataStage := atomic.Bool{}
		reachedDataStageChan := make(chan struct{})
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if isDownloadOrRestoreEndpoint(req.URL.Path) || isLasOrRestoreEndpoint(req.URL.Path) {
				if reachedDataStage.CompareAndSwap(false, true) {
					close(reachedDataStageChan)
				}
				time.Sleep(time.Second)
				return nil, nil
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.TaskID = uuid.NewTime()
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			t.Fatal(errors.Wrap(err, "marshal properties"))
		}
		ctx, cancel := context.WithCancel(context.Background())
		res := make(chan error)
		go func() {
			res <- h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
		}()

		Print("Wait for data stage")
		select {
		case <-reachedDataStageChan:
			cancel()
		case err := <-res:
			t.Fatalf("Restore finished before reaching data stage with: %s", err)
		}

		Print("Validate restore was paused in time")
		select {
		case err := <-res:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Expected restore to end with context cancelled, got %q", err)
			}
		case <-time.NewTimer(2 * time.Second).C:
			t.Fatal("Restore wasn't paused in time")
		}
	})
}

func TestRestoreTablesMultiLocationIntegration(t *testing.T) {
	// Since we need multi-dc clusters for multi-dc backup/restore
	// we will use the same cluster as both src and dst.
	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ks := randomizedName("multi_location_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Save filled table into map")
	srcM := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ks, tab, "id", "data")

	Print("Run backup")
	loc := []backupspec.Location{
		testLocation("multi-location-1", "dc1"),
		testLocation("multi-location-2", "dc2"),
	}
	S3InitBucket(t, loc[0].Path)
	S3InitBucket(t, loc[1].Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(backupspec.Location{}, ks)
	backupProps["location"] = loc
	backupProps["batch_size"] = 100
	tag := h.runBackup(t, backupProps)

	Print("Truncate backed up table")
	truncateStmt := "TRUNCATE TABLE %q.%q"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(truncateStmt, ks, tab))

	// Reverse dcs - just for fun
	loc[0].DC = "dc2"
	loc[1].DC = "dc1"

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	go func() {
		props := defaultTestProperties(backupspec.Location{}, tag, true)
		props["location"] = loc
		props["keyspace"] = ksFilter
		props["parallel"] = 1 // Test if batching does not hang with limited parallel and location access
		h.runRestore(t, props)
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstM := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ks, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcM, dstM) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcM, dstM)
	}
}

func TestRestoreTablesProgressIntegration(t *testing.T) {
	// It verifies that:
	// - view status progress is correct
	// - progress is available even when cluster is not

	if IsIPV6Network() {
		t.Skip("nodes don't have ip6tables and related modules to properly simulate unavailable cluster")
	}

	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ks := randomizedName("progress_")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))

	Print("Table setup")
	tab := randomizedName("tab_")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	// It's not possible to create views on tablet keyspaces
	tabToValidate := []string{tab}
	rd := scyllaclient.NewRingDescriber(context.Background(), h.srcCluster.Client)
	if !rd.IsTabletKeyspace(ks) {
		Print("View setup")
		mv := randomizedName("mv_")
		CreateMaterializedView(t, h.srcCluster.rootSession, ks, tab, mv)
		CreateMaterializedView(t, h.dstCluster.rootSession, ks, tab, mv)
		tabToValidate = append(tabToValidate, mv)
	}

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 1, ks, tab)

	Print("Run backup")
	loc := testLocation("progress", "")
	S3InitBucket(t, loc.Path)
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	props := defaultTestProperties(loc, tag, true)
	h.runRestore(t, props)

	Print("Validate success")
	for _, tab := range tabToValidate {
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab, "id", "data")
	}

	Print("Validate view progress")
	pr, err := h.dstRestoreSvc.GetProgress(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	for _, v := range pr.Views {
		if v.BuildStatus != scyllaclient.StatusSuccess {
			t.Fatalf("Expected status: %s, got: %s", scyllaclient.StatusSuccess, v.BuildStatus)
		}
	}

	BlockREST(t, ManagedClusterHosts()...)
	defer func() {
		TryUnblockREST(t, ManagedClusterHosts())
		if err := EnsureNodesAreUP(t, ManagedClusterHosts(), time.Minute); err != nil {
			t.Fatal(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	Print("Validate view progress when cluster is unavailable")
	pr, err = h.dstRestoreSvc.GetProgress(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	for _, v := range pr.Views {
		if v.BuildStatus != scyllaclient.StatusSuccess {
			t.Fatalf("Expected status: %s, got: %s", scyllaclient.StatusSuccess, v.BuildStatus)
		}
	}
}

func TestRestoreOnlyOneDCFromLocationIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	Print("Keyspace setup")
	// Source cluster
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ksTwoDC := randomizedName("two_dc_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ksTwoDC))

	// Keyspace thats only available in dc2
	ksStmtOneDC := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1':0, 'dc2': 1}"
	ksOneDC := randomizedName("one_dc_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmtOneDC, ksOneDC))

	// Target cluster
	ksStmtDst := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}"
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmtDst, ksTwoDC))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmtDst, ksOneDC))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	for _, ks := range []string{ksTwoDC, ksOneDC} {
		ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
		ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	}

	Print("Fill setup")
	for _, ks := range []string{ksTwoDC, ksOneDC} {
		fillTable(t, h.srcCluster.rootSession, 100, ks, tab)
	}

	Print("Save filled table into map")
	srcMTwoDC := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ksTwoDC, tab, "id", "data")

	Print("Run backup")
	loc := testLocation("one-location-1", "")

	S3InitBucket(t, loc.Path)
	ksFilter := []string{ksTwoDC, ksOneDC}
	backupProps := defaultTestBackupProperties(loc, "")
	backupProps["keyspace"] = ksFilter
	backupProps["batch_size"] = 100
	tag := h.runBackup(t, backupProps)

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	go func() {
		props := defaultTestProperties(loc, tag, true)
		props["keyspace"] = ksFilter
		props["parallel"] = 1 // Test if batching does not hang with limited parallel and location access
		props["dc_mapping"] = map[string]string{
			"dc1": "dc1",
		}
		h.runRestore(t, props)
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstMTwoDC := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ksTwoDC, tab, "id", "data")
	dstMOneDC := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ksOneDC, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcMTwoDC, dstMTwoDC) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcMTwoDC, dstMTwoDC)
	}
	if len(dstMOneDC) != 0 {
		t.Fatalf("dc2 shouldn't be restored")
	}
}

func TestRestoreDCMappingsIntegration(t *testing.T) {
	// Since we need multi-dc clusters for multi-dc backup/restore
	// we will use the same cluster as both src and dst.
	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ks := randomizedName("multi_location_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Save filled table into map")
	srcM := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ks, tab, "id", "data")

	Print("Run backup")
	loc := []backupspec.Location{
		testLocation("dc-mapping-1", "dc1"),
		testLocation("dc-mapping-2", "dc2"),
	}
	S3InitBucket(t, loc[0].Path)
	S3InitBucket(t, loc[1].Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(backupspec.Location{}, ks)
	backupProps["location"] = loc
	backupProps["batch_size"] = 100
	tag := h.runBackup(t, backupProps)

	Print("Truncate backed up table")
	truncateStmt := "TRUNCATE TABLE %q.%q"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(truncateStmt, ks, tab))

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	dcMappings := map[string]string{
		"dc1": "dc1",
		"dc2": "dc2",
	}
	go func() {
		props := defaultTestProperties(backupspec.Location{}, tag, true)
		props["location"] = loc
		props["keyspace"] = ksFilter
		props["dc_mapping"] = dcMappings
		h.runRestore(t, props)
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstM := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ks, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcM, dstM) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcM, dstM)
	}

	Print("Ensure nodes downloaded tables only from corresponding DC")
	// Restore run progress has RemoteSSTableDir of downloaded table which contains dc name in the path.
	// We can compare this dc with the host dc (apply mappings) and they should be equal.
	restoreProgress := selectRestoreRunProgress(t, h)
	sourceDCByTargetDC := revertDCMapping(dcMappings)
	for _, pr := range restoreProgress {
		targetDC, err := h.dstCluster.Client.HostDatacenter(context.Background(), pr.Host)
		if err != nil {
			t.Fatalf("get host dc: %v", err)
		}
		expectedSourceDC, ok := sourceDCByTargetDC[targetDC]
		if !ok {
			t.Fatalf("mapping not found for target dc: %v", targetDC)
		}
		actualDC := getDCFromRemoteSSTableDir(t, pr.RemoteSSTableDir)

		if actualDC != expectedSourceDC {
			t.Fatalf("Host should download data only from %s, but downloaded from %s", expectedSourceDC, actualDC)
		}
	}
}

func TestRestoreTablesMethodIntegration(t *testing.T) {
	// This test validates that the correct API is used
	// for restoring batches (Rclone or Scylla).
	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())
	ctx := context.Background()

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("method_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Backup setup")
	loc := testLocation("method", "")
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(loc, ks)
	tag := h.runBackup(t, backupProps)
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)

	const (
		rcloneAPIPath = "/storage_service/sstables"
		nativeAPIPath = "/storage_service/restore"
	)

	ni, err := h.dstCluster.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Note that this is just a test check - it does not reflect ni.SupportsNativeRestoreAPI().
	nativeRestoreSupport, err := version.CheckConstraint(ni.ScyllaVersion, ">= 2025.2")
	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		method           restore.Method
		blockedPath      string
		ensuredPath      string
		getTargetSuccess bool
	}
	var testCases []testCase
	// As currently scylla can't handle ipv6 object storage endpoints,
	// we don't configure them for ipv6 test env and don't expect them to work.
	switch {
	case nativeRestoreSupport && !IsIPV6Network():
		testCases = []testCase{
			{method: restore.MethodAuto, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{method: restore.MethodNative, ensuredPath: nativeAPIPath, blockedPath: rcloneAPIPath, getTargetSuccess: true},
			{method: restore.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	case nativeRestoreSupport && IsIPV6Network():
		testCases = []testCase{
			{method: restore.MethodAuto, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{method: restore.MethodNative, getTargetSuccess: false},
			{method: restore.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	default:
		testCases = []testCase{
			{method: restore.MethodAuto, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{method: restore.MethodNative, getTargetSuccess: false},
			{method: restore.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Test case: %s, %s, %s, %v", tc.method, tc.ensuredPath, tc.blockedPath, tc.getTargetSuccess), func(t *testing.T) {
			if err := h.dstCluster.rootSession.ExecStmt(fmt.Sprintf("TRUNCATE TABLE %q.%q", ks, tab)); err != nil {
				t.Fatal(err)
			}

			encounteredEnsured := atomic.Bool{}
			encounteredBlocked := atomic.Bool{}
			h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if strings.HasPrefix(req.URL.Path, tc.ensuredPath) {
					encounteredEnsured.Store(true)
				}
				if strings.HasPrefix(req.URL.Path, tc.blockedPath) {
					encounteredBlocked.Store(true)
				}
				return nil, nil
			}))

			h.dstCluster.RunID = uuid.NewTime()
			props := defaultTestProperties(loc, tag, true)
			props["keyspace"] = ksFilter
			props["method"] = tc.method
			if tc.method == "" {
				delete(props, "method")
			}
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}

			if !tc.getTargetSuccess {
				_, _, _, err := h.dstRestoreSvc.GetTargetUnitsViews(ctx, h.dstCluster.ClusterID, rawProps)
				if err != nil {
					return
				}
				t.Fatal("Expected GetTargetUnitsViews to fail")
			}

			err = h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
			if err != nil {
				t.Fatal(err)
			}

			if !encounteredEnsured.Load() {
				t.Fatalf("Expected SM to use %q API", tc.ensuredPath)
			}
			if encounteredBlocked.Load() {
				t.Fatalf("Expected SM not to use %q API", tc.blockedPath)
			}

			h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
		})
	}
}

func TestRestoreFullAlternatorIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ni, err := h.srcCluster.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := ni.SupportsAlternatorSchemaBackupFromAPI(); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Skip("This test assumes that alternator tables are backed up and restored with alternator api")
	}

	Print("Prepare alternator schema")
	altTab1 := "alt_full_1_" + AlternatorProblematicTableChars
	altTab2 := "alt_full_2_" + AlternatorProblematicTableChars
	altGSI1 := "gsi_1_" + AlternatorProblematicTableChars
	altGSI2 := "gsi_2_" + AlternatorProblematicTableChars
	CreateAlternatorTable(t, h.srcCluster.altClient, 2, altTab1, altTab2)
	CreateAlternatorGSI(t, h.srcCluster.altClient, altTab1, altGSI1, altGSI2)
	CreateAlternatorGSI(t, h.srcCluster.altClient, altTab2, altGSI1, altGSI2)

	Print("Insert alternator rows")
	const rowCnt = 100
	InsertAlternatorTableData(t, h.srcCluster.altClient, rowCnt, altTab1, altTab2)

	Print("Prepare simple clq schema")
	ExecStmt(t, h.srcCluster.rootSession, "CREATE KEYSPACE cql_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2} and tablets = {'enabled': false}")
	createTable(t, h.srcCluster.rootSession, "cql_ks", "cql_tab")
	CreateMaterializedView(t, h.srcCluster.rootSession, "cql_ks", "cql_tab", "cql_mv_1")
	CreateMaterializedView(t, h.srcCluster.rootSession, "cql_ks", "cql_tab", "cql_mv_2")
	CreateSecondaryIndex(t, h.srcCluster.rootSession, "cql_ks", "cql_tab", "cql_si_1")

	Print("Insert simple cql rows")
	fillTable(t, h.srcCluster.rootSession, rowCnt, "cql_ks", "cql_tab")

	Print("Run backup")
	loc := testLocation("alternator-full", "")
	S3InitBucket(t, loc.Path)
	backupProps := defaultTestBackupProperties(loc, "")
	tag := h.runBackup(t, backupProps)

	Print("Restore schema")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	props := defaultTestProperties(loc, tag, false)
	h.runRestore(t, props)

	Print("Reset user permissions")
	dropNonSuperUsers(t, h.dstCluster.rootSession)
	createUser(t, h.dstCluster.rootSession, h.dstUser, h.dstPass)

	Print("Restore data")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	props = defaultTestProperties(loc, tag, true)
	h.runRestore(t, props)

	Print("Validate restored alternator data")
	ValidateAlternatorTableData(t, h.dstCluster.altClient, rowCnt, 2, altTab1, altTab2)
	ValidateAlternatorGSIData(t, h.dstCluster.altClient, rowCnt, altTab1, altGSI1, altGSI2)
	ValidateAlternatorGSIData(t, h.dstCluster.altClient, rowCnt, altTab2, altGSI1, altGSI2)

	Print("Validate restored simple cql data")
	cqlTabs := []table{
		{ks: "cql_ks", tab: "cql_tab"},
		{ks: "cql_ks", tab: "cql_mv_1"},
		{ks: "cql_ks", tab: "cql_mv_2"},
		{ks: "cql_ks", tab: "cql_si_1_index"},
	}
	for _, tab := range cqlTabs {
		srcCnt := rowCount(t, h.srcCluster.rootSession, tab.ks, tab.tab)
		dstCnt := rowCount(t, h.dstCluster.rootSession, tab.ks, tab.tab)
		if srcCnt != dstCnt {
			t.Fatalf("Expected %d rows in cql table %q.%q, got %d", srcCnt, tab.ks, tab.tab, dstCnt)
		}
	}
}

func getDCFromRemoteSSTableDir(t *testing.T, remoteSSTableDir string) string {
	t.Helper()
	// hacky way of extracting value of dc_name from  /dc/{dc_name}/node/
	var re = regexp.MustCompile(`\/dc\/(.*?)\/node\/`)
	matches := re.FindStringSubmatch(remoteSSTableDir)
	if len(matches) != 2 {
		t.Fatalf("Unexpected remote sstable dir format: %s", remoteSSTableDir)
	}

	return matches[1]
}

func revertDCMapping(dcMapping map[string]string) map[string]string {
	result := make(map[string]string, len(dcMapping))
	for k, v := range dcMapping {
		result[v] = k
	}
	return result
}

func selectRestoreRunProgress(t *testing.T, h *testHelper) []restore.RunProgress {
	t.Helper()
	q := schematable.RestoreRunProgress.SelectQuery(h.dstCluster.Session)
	it := q.BindMap(qb.M{
		"cluster_id": h.dstCluster.ClusterID,
		"task_id":    h.dstCluster.TaskID,
		"run_id":     h.dstCluster.RunID,
	}).Iter()
	defer q.Release()
	var (
		result []restore.RunProgress
		row    restore.RunProgress
	)
	for it.StructScan(&row) {
		result = append(result, row)
		row = restore.RunProgress{}
	}
	return result
}
