// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
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

	Print("Run restore of src backup")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save dst describe schema output from src backup")
	dstSchemaSrcBackup, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe dst schema from src backup"))
	}

	Print("Run dst backup")
	tag = hRev.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Drop restored schema")
	ExecStmt(t, h.dstCluster.rootSession, "DROP KEYSPACE "+ks)

	Print("Run restore of dst backup")
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save dst describe schema output from dst backup")
	dstSchemaDstBackup, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe dst schema from dst backup"))
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
	for _, row := range dstSchemaDstBackup {
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
