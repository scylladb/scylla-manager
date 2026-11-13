// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package snapshot_test

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/nopmigrate"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/schema"
	"github.com/scylladb/scylla-manager/v3/schema/snapshot"
)

const keyspace = "test_scylla_manager"

// state is everything Apply is expected to reproduce from a full replay.
type state struct {
	Schema     map[string]string
	Migrations map[string]string
}

// Snapshot plus newer migrations must give the same schema and migration history as a full replay.
func TestSnapshotMatchesMigrationsIntegration(t *testing.T) {
	if testutils.UpdateGoldenFiles() {
		s := db.CreateSessionWithoutMigration(t)
		defer s.Close()
		runMigrations(t, s, migrationsUpTo(t, snapshot.Version, true))
		writeSnapshot(t, s)
		t.Skip("schema.cql regenerated, rerun without -update")
	}
	want := replayState(t)

	s := db.CreateSessionWithoutMigration(t)
	defer s.Close()
	apply(t, s, schema.Files)
	// Must find every snapshotted migration already applied and run only newer ones.
	runMigrations(t, s, schema.Files)
	// Second Apply must be a no-op.
	apply(t, s, schema.Files)
	if diff := cmp.Diff(want, dump(t, s)); diff != "" {
		t.Fatalf("schema.cql is out of sync with migrations, regenerate it (-replay +snapshot):\n%s", diff)
	}
}

// Apply must leave a keyspace that already has migration history untouched.
func TestApplyIsNoopOnMigratedKeyspaceIntegration(t *testing.T) {
	s := db.CreateSessionWithoutMigration(t)
	defer s.Close()
	runMigrations(t, s, schema.Files)
	want := dump(t, s)

	apply(t, s, schema.Files)
	if diff := cmp.Diff(want, dump(t, s)); diff != "" {
		t.Fatalf("Apply changed a migrated keyspace (-before +after):\n%s", diff)
	}
}

// Crash after some schema statements but before migrations are marked as applied
// must not make the next start replay all migrations on a half built schema.
func TestApplyRecoversFromInterruptedRunIntegration(t *testing.T) {
	want := replayState(t)

	s := db.CreateSessionWithoutMigration(t)
	defer s.Close()
	b, err := os.ReadFile("schema.cql")
	if err != nil {
		t.Fatal(err)
	}
	stmts := snapshot.SplitStmts(b)
	if len(stmts) < 10 {
		t.Fatalf("unexpected number of statements in schema.cql: %d", len(stmts))
	}
	for _, stmt := range stmts[:len(stmts)/2] {
		if err := s.ExecStmt(stmt); err != nil {
			t.Fatal(err)
		}
	}

	apply(t, s, schema.Files)
	runMigrations(t, s, schema.Files)
	if diff := cmp.Diff(want, dump(t, s)); diff != "" {
		t.Fatalf("state after recovery differs from replay (-replay +recovered):\n%s", diff)
	}
}

// Keyspace with a table or type that is not in schema.cql, or one defined differently, must be left to migrations.
func TestApplySkipsKeyspaceWithForeignSchemaIntegration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stmts []string
	}{
		{"table", []string{"CREATE TABLE foreign_table (id int PRIMARY KEY)"}},
		{"table with same name and other columns", []string{"CREATE TABLE scheduler_task (cluster_id uuid, type text, id uuid, PRIMARY KEY (cluster_id, type, id))"}},
		{"type", []string{"CREATE TYPE foreign_type (a int)"}},
		{"type with same name and other fields", []string{"CREATE TYPE token_range (start_token bigint, end_token text)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := db.CreateSessionWithoutMigration(t)
			defer s.Close()
			for _, stmt := range tc.stmts {
				if err := s.ExecStmt(stmt); err != nil {
					t.Fatal(err)
				}
			}

			apply(t, s, schema.Files)
			got := dump(t, s)
			if len(got.Migrations) != 0 {
				t.Fatalf("Apply must not mark migrations, got %d rows", len(got.Migrations))
			}
			if _, ok := got.Schema["column backup_run.id"]; ok {
				t.Fatal("Apply must not create snapshot tables")
			}
		})
	}
}

// Keyspace created by an older Manager must be upgraded by migrations, not touched by the snapshot.
func TestUpgradeFromOlderVersionIntegration(t *testing.T) {
	want := replayState(t)

	old := migrationsUpTo(t, snapshot.Version, false)

	s := db.CreateSessionWithoutMigration(t)
	defer s.Close()
	runMigrations(t, s, old)
	apply(t, s, schema.Files)
	if got := len(dump(t, s).Migrations); got != len(old) {
		t.Fatalf("Apply must not mark migrations on an old keyspace, got %d rows, want %d", got, len(old))
	}
	runMigrations(t, s, schema.Files)
	if diff := cmp.Diff(want, dump(t, s)); diff != "" {
		t.Fatalf("upgraded state differs from replay (-replay +upgraded):\n%s", diff)
	}
}

// migrationsUpTo returns migrations before last, and last itself if inclusive.
func migrationsUpTo(t *testing.T, last string, inclusive bool) fstest.MapFS {
	t.Helper()
	names, err := fs.Glob(schema.Files, "*.cql")
	if err != nil {
		t.Fatal(err)
	}
	out := fstest.MapFS{}
	for _, name := range names {
		if name > last || (name == last && !inclusive) {
			break
		}
		b, err := fs.ReadFile(schema.Files, name)
		if err != nil {
			t.Fatal(err)
		}
		out[name] = &fstest.MapFile{Data: b}
	}
	if len(out) == 0 || len(out) == len(names) && !inclusive {
		t.Fatalf("need a partial migration set, got %d of %d", len(out), len(names))
	}
	return out
}

// replayed caches replayState, a full replay is the slowest step and its result never changes.
var replayed *state

func replayState(t *testing.T) state {
	t.Helper()
	if replayed == nil {
		s := db.CreateSessionWithoutMigration(t)
		defer s.Close()
		runMigrations(t, s, schema.Files)
		st := dump(t, s)
		replayed = &st
	}
	return *replayed
}

func apply(t *testing.T, s gocqlx.Session, files fs.FS) {
	t.Helper()
	if err := snapshot.Apply(t.Context(), s, files, log.NopLogger); err != nil {
		t.Fatal("snapshot:", err)
	}
}

func runMigrations(t *testing.T, s gocqlx.Session, files fs.FS) {
	t.Helper()
	migrate.Callback = nopmigrate.Callback
	if err := migrate.FromFS(t.Context(), s, files); err != nil {
		t.Fatal("migrate:", err)
	}
	// Mirrors fixSchedulerTaskTTL, which SM runs after migrations.
	if err := s.ExecStmt("ALTER TABLE scheduler_task WITH default_time_to_live = 0"); err != nil {
		t.Fatal(err)
	}
}

func dump(t *testing.T, s gocqlx.Session) state {
	t.Helper()
	st := state{Schema: dumpSchema(t, s), Migrations: make(map[string]string)}
	infos, err := migrate.List(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range infos {
		st.Migrations[i.Name] = fmt.Sprint(i.Checksum, i.Done)
	}
	return st
}

func dumpSchema(t *testing.T, s gocqlx.Session) map[string]string {
	t.Helper()
	out := make(map[string]string)
	var tbl, col, kind, order, typ string
	var pos int
	iter := s.Query("SELECT table_name, column_name, kind, position, clustering_order, type FROM system_schema.columns WHERE keyspace_name = ?", nil).Bind(keyspace).Iter()
	for iter.Scan(&tbl, &col, &kind, &pos, &order, &typ) {
		out["column "+tbl+"."+col] = fmt.Sprint(kind, pos, order, typ)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	var fields, types []string
	iter = s.Query("SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = ?", nil).Bind(keyspace).Iter()
	for iter.Scan(&tbl, &fields, &types) {
		out["type "+tbl] = fmt.Sprint(fields, types)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	// Every property, so that options trimmed from schema.cql are caught too.
	for _, o := range []struct{ kind, table, name string }{
		{"table", "tables", "table_name"},
		{"index", "indexes", "index_name"},
		{"view", "views", "view_name"},
	} {
		iter := s.Session.Query("SELECT * FROM system_schema."+o.table+" WHERE keyspace_name = ?", keyspace).Iter()
		for row := map[string]any{}; iter.MapScan(row); row = map[string]any{} {
			name := fmt.Sprint(row[o.name])
			delete(row, "id")
			out[o.kind+" "+name] = fmt.Sprint(row)
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

const snapshotHeader = `-- Schema produced by applying all migrations up to and including Version (see snapshot.go).
-- Used to initialize an empty keyspace in one step instead of replaying every migration.
-- Generated, do not edit: bump Version and run TestSnapshotMatchesMigrationsIntegration with -update.
`

// writeSnapshot regenerates schema.cql from s, with one column or field per line as snapshot.go expects.
// Emits only types, tables, CLUSTERING ORDER and TTL; extend it (not schema.cql) when migrations add indexes, views or options.
func writeSnapshot(t *testing.T, s gocqlx.Session) {
	t.Helper()
	var b strings.Builder
	b.WriteString(snapshotHeader)

	types := make(map[string][]string)
	var name string
	var names, kinds []string
	iter := s.Query("SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = ?", nil).Bind(keyspace).Iter()
	for iter.Scan(&name, &names, &kinds) {
		for i := range names {
			types[name] = append(types[name], names[i]+" "+kinds[i])
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	// Types used by other types go first.
	for done := map[string]bool{}; len(done) < len(types); {
		progress := false
		for _, n := range slices.Sorted(maps.Keys(types)) {
			if done[n] || dependsOnPending(types[n], types, done, n) {
				continue
			}
			fmt.Fprintf(&b, "\nCREATE TYPE IF NOT EXISTS %s (\n    %s\n);\n", n, strings.Join(types[n], ",\n    "))
			done[n] = true
			progress = true
		}
		if !progress {
			t.Fatal("cyclic type dependencies")
		}
	}

	type column struct {
		name, kind, order, typ string
		pos                    int
	}
	cols := make(map[string][]column)
	var c column
	var tbl string
	iter = s.Query("SELECT table_name, column_name, kind, position, clustering_order, type FROM system_schema.columns WHERE keyspace_name = ?", nil).Bind(keyspace).Iter()
	for iter.Scan(&tbl, &c.name, &c.kind, &c.pos, &c.order, &c.typ) {
		cols[tbl] = append(cols[tbl], c)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	ttls := make(map[string]int)
	var ttl int
	iter = s.Query("SELECT table_name, default_time_to_live FROM system_schema.tables WHERE keyspace_name = ?", nil).Bind(keyspace).Iter()
	for iter.Scan(&tbl, &ttl) {
		ttls[tbl] = ttl
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	delete(cols, "gocqlx_migrate")

	rank := map[string]int{"partition_key": 0, "clustering": 1, "static": 2, "regular": 2}
	for _, tbl := range slices.Sorted(maps.Keys(cols)) {
		cs := cols[tbl]
		slices.SortFunc(cs, func(a, b column) int {
			if d := rank[a.kind] - rank[b.kind]; d != 0 {
				return d
			}
			if a.kind == "partition_key" || a.kind == "clustering" {
				return a.pos - b.pos
			}
			return strings.Compare(a.name, b.name)
		})
		var pk, ck, order []string
		fmt.Fprintf(&b, "\nCREATE TABLE IF NOT EXISTS %s (\n", tbl)
		for _, c := range cs {
			static := ""
			switch c.kind {
			case "partition_key":
				pk = append(pk, c.name)
			case "clustering":
				ck = append(ck, c.name)
				order = append(order, c.name+" "+strings.ToUpper(c.order))
			case "static":
				static = " static"
			}
			fmt.Fprintf(&b, "    %s %s%s,\n", c.name, c.typ, static)
		}
		key := strings.Join(pk, ", ")
		if len(pk) > 1 {
			key = "(" + key + ")"
		}
		fmt.Fprintf(&b, "    PRIMARY KEY (%s)\n)", strings.Join(append([]string{key}, ck...), ", "))
		var opts []string
		if len(order) > 0 {
			opts = append(opts, "CLUSTERING ORDER BY ("+strings.Join(order, ", ")+")")
		}
		if ttls[tbl] != 0 {
			opts = append(opts, fmt.Sprint("default_time_to_live = ", ttls[tbl]))
		}
		if len(opts) > 0 {
			b.WriteString(" WITH " + strings.Join(opts, " AND "))
		}
		b.WriteString(";\n")
	}

	if err := os.WriteFile("schema.cql", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func dependsOnPending(fields []string, types map[string][]string, done map[string]bool, self string) bool {
	for other := range types {
		if other == self || done[other] {
			continue
		}
		re := regexp.MustCompile(`\b` + other + `\b`)
		for _, f := range fields {
			if _, typ, _ := strings.Cut(f, " "); re.MatchString(typ) {
				return true
			}
		}
	}
	return false
}
