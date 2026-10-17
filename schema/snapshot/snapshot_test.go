// Copyright (C) 2026 ScyllaDB

package snapshot

import (
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/schema"
)

func TestVersionIsAMigration(t *testing.T) {
	names, err := fs.Glob(schema.Files, "*.cql")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(names, Version) {
		t.Fatalf("Version %s is not in schema migrations", Version)
	}
}

// Statements must be idempotent so that Apply can be retried after a failure.
func TestSchemaStatementsAreIdempotent(t *testing.T) {
	re := regexp.MustCompile(`^(?s:(--[^\n]*\n|\s)*)CREATE (TABLE|TYPE) IF NOT EXISTS \w+ \(`)
	stmts := splitStmts([]byte(schemaCQL))
	if len(stmts) == 0 {
		t.Fatal("schema.cql has no statements")
	}
	for _, s := range stmts {
		if !re.MatchString(s) {
			t.Errorf("statement is not CREATE TABLE/TYPE IF NOT EXISTS: %q", s)
		}
	}
}

// foreignSchema relies on tableRe and typeRe to know every table and type of schema.cql.
func TestRegexpsFindAllObjects(t *testing.T) {
	tables := snapshotTables()
	if want := strings.Count(schemaCQL, "CREATE TABLE"); len(tables) != want || want == 0 {
		t.Fatalf("tableRe found %d tables, schema.cql has %d", len(tables), want)
	}
	if got := tables["scheduler_task"]; !slices.Contains(got, "sched frozen<schedule>") || slices.ContainsFunc(got, func(c string) bool {
		return strings.Contains(c, "PRIMARY KEY")
	}) {
		t.Fatalf("unexpected scheduler_task columns: %q", got)
	}
	types := snapshotTypes()
	if want := strings.Count(schemaCQL, "CREATE TYPE"); len(types) != want || want == 0 {
		t.Fatalf("typeRe found %d types, schema.cql has %d", len(types), want)
	}
	if got := types["restore_unit"]; !slices.Equal(got, []string{"keyspace_name text", "size bigint", "tables set<frozen<restore_table>>"}) {
		t.Fatalf("unexpected restore_unit fields: %q", got)
	}
}

func TestSplitStmts(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", 0},
		{"whitespace only", " \n", 0},
		{"one with semicolon", "CREATE TABLE a (id int PRIMARY KEY);", 1},
		{"one without semicolon", "CREATE TABLE a (id int PRIMARY KEY)", 1},
		{"two", "A;\nB;\n", 2},
		{"comment before", "-- c\nA;\n-- d\nB;", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(splitStmts([]byte(tc.in))); got != tc.want {
				t.Fatalf("got %d statements, want %d", got, tc.want)
			}
		})
	}
}
