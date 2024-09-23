// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestRestoreTablesUserIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if checkAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("Auth restore is not supported in Scylla 6.0. It requires core side support that is aimed at 6.1 release")
	}

	user := randomizedName("user_")
	pass := randomizedName("pass_")
	Printf("Create user (%s/%s) to be backed-up", user, pass)
	createUser(t, h.srcCluster.rootSession, user, pass)
	ExecStmt(t, h.srcCluster.rootSession, "GRANT CREATE ON ALL KEYSPACES TO "+user)

	Print("Run backup")
	loc := []Location{testLocation("user", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

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
	loc := []Location{testLocation("no-replication", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreSchemaRoundtripIntegration(t *testing.T) {
	// Test scenario:
	// - create schema on src cluster
	// - back up src cluster
	// - restore src cluster schema to dst cluster
	// - drop src cluster schema
	// - back up dst cluster
	// - restore dst cluster schema to src cluster
	// - validates that schema was correct at all stages
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())
	hRev := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	if !checkAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("This test assumes that schema is backed up and restored via DESCRIBE SCHEMA WITH INTERNALS")
	}

	ks := randomizedName("roundtrip_")
	tab := randomizedName("tab_")
	Print("Prepare schema with non-default options")
	ksOpt := "durable_writes = false"
	tabOpt := "compaction = {'class': 'NullCompactionStrategy', 'enabled': 'false'}"
	objWithOpt := map[string]string{
		ks:  ksOpt,
		tab: tabOpt,
	}
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} AND %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2, ksOpt))
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data blob) WITH %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, tabOpt))

	Print("Save src describe schema output")
	srcSchema, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe src schema"))
	}

	Print("Run src backup")
	loc := []Location{testLocation("schema-roundtrip", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Drop backed-up src cluster schema")
	ExecStmt(t, h.srcCluster.rootSession, "DROP KEYSPACE "+ks)

	Print("Run restore of src backup on dst cluster")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save dst describe schema output from src backup")
	dstSchemaSrcBackup, err := query.DescribeSchemaWithInternals(h.dstCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe dst schema from src backup"))
	}

	Print("Run dst backup")
	tag = hRev.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Run restore of dst backup on src cluster")
	grantRestoreSchemaPermissions(t, hRev.dstCluster.rootSession, hRev.dstUser)
	hRev.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save src describe schema output from dst backup")
	srcSchemaDstBackup, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe src schema from dst backup"))
	}

	Print("Validate that schema contains objects with options")
	var (
		m1 = map[query.DescribedSchemaRow]struct{}{}
		m2 = map[query.DescribedSchemaRow]struct{}{}
		m3 = map[query.DescribedSchemaRow]struct{}{}
	)
	for _, row := range srcSchema {
		m1[row] = struct{}{}
		if opt, ok := objWithOpt[row.Name]; ok {
			if !strings.Contains(row.CQLStmt, opt) {
				t.Fatalf("Object: %v, with cql: %v, does not contain option: %v", row.Name, row.CQLStmt, opt)
			}
			delete(objWithOpt, row.Name)
		}
	}
	if len(objWithOpt) > 0 {
		t.Fatalf("Src schema: %v, is missing created objects: %v", m1, objWithOpt)
	}
	for _, row := range dstSchemaSrcBackup {
		m2[row] = struct{}{}
	}
	for _, row := range srcSchemaDstBackup {
		m3[row] = struct{}{}
	}
	Print("Validate that all schemas are the same")
	if !maputil.Equal(m1, m2) {
		t.Fatalf("Src schema: %v, dst schema from src backup: %v, are not equal", m1, m2)
	}
	if !maputil.Equal(m1, m3) {
		t.Fatalf("Src schema: %v, dst schema from dst backup: %v, are not equal", m1, m3)
	}
}

func TestRestoreSchemaDropAddColumnIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if !checkAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
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
	loc := []Location{testLocation("drop-add", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Run restore schema")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

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
	loc := []Location{testLocation("vnode-to-tablets", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Manually recreate tablet schema")
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 3, true))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, c1, c2))

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

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

	Print("View setup")
	mv := randomizedName("mv_")
	CreateMaterializedView(t, h.srcCluster.rootSession, ks1, tab, mv)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks1, tab, mv)
	si := randomizedName("si_")
	CreateSecondaryIndex(t, h.srcCluster.rootSession, ks1, tab, si)
	CreateSecondaryIndex(t, h.dstCluster.rootSession, ks1, tab, si)
	mv1 := randomizedName("mv_1_")
	CreateMaterializedView(t, h.srcCluster.rootSession, ks2, tab1, mv1)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks2, tab1, mv1)

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks1, tab)
	fillTable(t, h.srcCluster.rootSession, 100, ks2, tab1, tab2)

	units := []backup.Unit{
		{
			Keyspace:  ks1,
			Tables:    []string{tab, mv, si + "_index"},
			AllTables: true,
		},
		{
			Keyspace:  ks2,
			Tables:    []string{tab1, tab2, mv1},
			AllTables: true,
		},
	}

	Print("Run backup")
	loc := []Location{testLocation("paused", "")}
	S3InitBucket(t, loc[0].Path)

	// Starting from SM 3.3.1, SM does not allow to back up views,
	// but backed up views should still be tested as older backups might
	// contain them. That's why here we manually force backup target
	// to contain the views.
	ctx := context.Background()
	h.srcCluster.TaskID = uuid.NewTime()
	h.srcCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(map[string]any{"location": loc})
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
	props := map[string]any{
		"location":       loc,
		"keyspace":       []string{ks1, ks2},
		"snapshot_tag":   tag,
		"restore_tables": true,
	}
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
