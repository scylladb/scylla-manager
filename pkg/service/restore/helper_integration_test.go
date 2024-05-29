// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
)

type table struct {
	ks  string
	tab string
}

func defaultTestConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   5,
		LongPollingTimeoutSeconds: 1,
	}
}

func randomizedName(name string) string {
	return name + strings.Replace(fmt.Sprint(uuid.NewTime()), "-", "", -1)
}

func tombstoneGCMode(t *testing.T, s gocqlx.Session, keyspace, table string) string {
	var ext map[string]string
	q := qb.Select("system_schema.tables").
		Columns("extensions").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(s).
		Bind(keyspace, table)

	defer q.Release()
	if err := q.Scan(&ext); err != nil {
		t.Fatal(errors.Wrap(err, "get table extensions"))
	}

	// Timeout (just using gc_grace_seconds) is the default mode
	mode, ok := ext["tombstone_gc"]
	if !ok {
		return "timeout"
	}

	allModes := []string{"disabled", "timeout", "repair", "immediate"}
	for _, m := range allModes {
		if strings.Contains(mode, m) {
			return m
		}
	}

	t.Fatal(errors.New("unknown mode " + mode))
	return ""
}

// baseTable returns view's base table or "" if it's not a view.
func baseTable(t *testing.T, s gocqlx.Session, keyspace, table string) string {
	q := qb.Select("system_schema.views").
		Columns("base_table_name").
		Where(qb.Eq("keyspace_name")).
		Where(qb.Eq("view_name")).Query(s).BindMap(qb.M{
		"keyspace_name": keyspace,
		"view_name":     table,
	})
	defer q.Release()

	var bt string
	if err := q.Scan(&bt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return ""
		}
		t.Fatal(errors.Wrap(err, "get base table"))
	}
	return bt
}

func rowCount(t *testing.T, s gocqlx.Session, ks, tab string) int {
	var cnt int
	if err := s.Session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %q.%q USING TIMEOUT 300s", ks, tab)).Scan(&cnt); err != nil {
		t.Fatal(errors.Wrapf(err, "get table %s.%s row count", ks, tab))
	}
	Printf("%s.%s row count: %v", ks, tab, cnt)
	return cnt
}

func filteredTables(t *testing.T, s gocqlx.Session, filter []string) []string {
	f, err := ksfilter.NewFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	var (
		ks, tab string
		out     []string
	)
	it := s.Session.Query("SELECT keyspace_name, table_name FROM system_schema.tables").Iter()
	for it.Scan(&ks, &tab) {
		if f.Check(ks, tab) {
			out = append(out, ks+"."+tab)
		}
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func dropNonSuperUsers(t *testing.T, s gocqlx.Session) {
	var (
		name  string
		super bool
	)
	iter := s.Query("LIST USERS", nil).Iter()
	for iter.Scan(&name, &super) {
		if !super {
			if err := s.ExecStmt(fmt.Sprintf("DROP USER '%s'", name)); err != nil {
				t.Fatal(errors.Wrapf(err, "drop user %s", name))
			}
		}
	}
}

func createUser(t *testing.T, s gocqlx.Session, user, pass string) {
	if err := s.ExecStmt(fmt.Sprintf("CREATE USER '%s' WITH PASSWORD '%s'", user, pass)); err != nil {
		t.Fatal(errors.Wrapf(err, "create user %s with pass %s", user, pass))
	}
}

func grantRestoreTablesPermissions(t *testing.T, s gocqlx.Session, restoredTablesFilter []string, user string) {
	f, err := ksfilter.NewFilter(restoredTablesFilter)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create filter"))
	}

	var ks, tab string
	iter := s.Query("SELECT keyspace_name, table_name FROM system_schema.tables", nil).Iter()
	for iter.Scan(&ks, &tab) {
		// Regular tables require ALTER permission
		if f.Check(ks, tab) {
			if err = s.ExecStmt(fmt.Sprintf("GRANT ALTER ON %q.%q TO '%s'", ks, tab, user)); err != nil {
				t.Fatal(errors.Wrapf(err, "grant alter on %s.%s", ks, tab))
			}
		}

		// Views of restored base tables require DROP and CREATE permissions
		if bt := baseTable(t, s, ks, tab); bt != "" {
			if f.Check(ks, bt) {
				if err = s.ExecStmt(fmt.Sprintf("GRANT DROP ON %q.%q TO '%s'", ks, bt, user)); err != nil {
					t.Fatal(errors.Wrapf(err, "grant drop on %s.%s", ks, tab))
				}
				if err = s.ExecStmt(fmt.Sprintf("GRANT CREATE ON %q TO '%s'", ks, user)); err != nil {
					t.Fatal(errors.Wrapf(err, "grant create on %s", ks))
				}
			}
		}
	}

	if err = iter.Close(); err != nil {
		t.Fatal(errors.Wrap(err, "close iterator"))
	}
}

func validateCompleteProgress(t *testing.T, pr Progress, tables []table) {
	if pr.Size != pr.Restored || pr.Size != pr.Downloaded {
		t.Fatal("Expected complete restore")
	}
	encountered := make(map[table]struct{})
	for _, kpr := range pr.Keyspaces {
		if kpr.Size != kpr.Restored || kpr.Size != kpr.Downloaded {
			t.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
		for _, tpr := range kpr.Tables {
			encountered[table{ks: kpr.Keyspace, tab: tpr.Table}] = struct{}{}
			if tpr.Size != tpr.Restored || tpr.Size != tpr.Downloaded {
				t.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
	}

	for _, tab := range tables {
		if _, ok := encountered[tab]; !ok {
			t.Fatalf("Table %s wasn't restored", tab)
		}
		delete(encountered, tab)
	}
	if len(encountered) > 0 {
		t.Fatalf("Restored more tables than expected: %v", encountered)
	}
}
