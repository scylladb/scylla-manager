// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

func TestOne2OneRestoreServiceIntegration(t *testing.T) {
	if tablets := os.Getenv("TABLETS"); tablets == "enabled" {
		t.Skip("1-1-restore is available only for v-nodes")
	}
	h := newTestHelper(t, ManagedClusterHosts())

	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.client)

	ksName := "testrestore"
	WriteData(t, clusterSession, ksName, 10)

	srcCnt := rowCount(t, clusterSession, ksName, BigTableName)
	if srcCnt == 0 {
		t.Fatalf("Unexpected row count: 0")
	}

	Print("Run backup")
	loc := []backupspec.Location{testLocation("1-1-restore", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Truncate tables")
	truncateAllTablesInKeyspace(t, clusterSession, ksName)
	if cnt := rowCount(t, clusterSession, ksName, BigTableName); cnt != 0 {
		t.Fatalf("Unexpected row count: %d", cnt)
	}

	Print("Run 1-1-restore")
	h.runRestore(t, map[string]any{
		"location":          loc,
		"snapshot_tag":      tag,
		"source_cluster_id": h.clusterID,
		"nodes_mapping":     getNodeMappings(t, h.client),
	})

	Print("Validate data")
	dstCnt := rowCount(t, clusterSession, ksName, BigTableName)
	if srcCnt != dstCnt {
		t.Fatalf("Expected row count %d, but got %d", srcCnt, dstCnt)
	}
}

func truncateAllTablesInKeyspace(tb testing.TB, session gocqlx.Session, ks string) {
	tb.Helper()

	q := qb.Select("system_schema.tables").Columns("table_name").Where(qb.Eq("keyspace_name")).Query(session).Bind(ks)
	defer q.Release()

	var all []string
	if err := q.Select(&all); err != nil {
		tb.Fatal(err)
	}

	for _, t := range all {
		truncateTable(tb, session, ks, t)
	}
}

func truncateTable(tb testing.TB, session gocqlx.Session, keyspace, table string) {
	tb.Helper()

	ExecStmt(tb, session, fmt.Sprintf("TRUNCATE TABLE %q.%q", keyspace, table))
}

func rowCount(t *testing.T, s gocqlx.Session, ks, tab string) int {
	var cnt int
	if err := s.Session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %q.%q USING TIMEOUT 300s", ks, tab)).Scan(&cnt); err != nil {
		t.Fatal(errors.Wrapf(err, "get table %s.%s row count", ks, tab))
	}
	Printf("%s.%s row count: %v", ks, tab, cnt)
	return cnt
}
