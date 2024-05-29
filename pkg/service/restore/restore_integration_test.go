// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

func TestRestoreTablesUserIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

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
