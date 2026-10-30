// Copyright (C) 2026 ScyllaDB

package snapshot

import (
	"bytes"
	"context"
	"crypto/md5"
	_ "embed"
	"encoding/hex"
	"io"
	"io/fs"
	"regexp"
	"slices"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// Version is the last migration file whose result is captured in schema.cql.
// Migrations after it are applied by migrate.FromFS as usual.
const Version = "v3.x13.0.cql"

//go:embed schema.cql
var schemaCQL string

var (
	tableRe = regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)`)
	typeRe  = regexp.MustCompile(`(?s)CREATE TYPE IF NOT EXISTS (\w+) \((.*?)\n\);`)
)

// Apply creates schema.cql in an empty keyspace and marks migrations up to Version as applied.
// Keyspaces already migrated or holding other schema are left to migrate.FromFS; idempotent, so safe to retry.
func Apply(ctx context.Context, session gocqlx.Session, files fs.FS, logger log.Logger) error {
	// Also ensures gocqlx_migrate exists, it's not part of schema.cql.
	// migrate.FromFS lists again; the extra round trip is accepted to keep Apply independent of it.
	applied, err := migrate.List(ctx, session)
	if err != nil {
		return errors.Wrap(err, "list migrations")
	}
	if len(applied) > 0 {
		logger.Debug(ctx, "Schema snapshot skipped", "reason", "keyspace already migrated")
		return nil
	}

	reason, err := foreignSchema(ctx, session)
	if err != nil {
		return err
	}
	if reason != "" {
		logger.Info(ctx, "Schema snapshot skipped, replaying all migrations", "reason", reason)
		return nil
	}

	names, err := fs.Glob(files, "*.cql")
	if err != nil {
		return err
	}
	slices.Sort(names)
	idx := slices.Index(names, Version)
	if idx < 0 {
		return errors.Errorf("snapshot version %s not found in migrations", Version)
	}

	for _, stmt := range splitStmts([]byte(schemaCQL)) {
		if err := session.ContextQuery(ctx, stmt, nil).ExecRelease(); err != nil {
			return errors.Wrapf(err, "exec %q", stmt)
		}
	}

	const insertStmt = "INSERT INTO gocqlx_migrate (name, checksum, done, start_time, end_time) VALUES (?, ?, ?, ?, ?)"
	now := timeutc.Now()
	// Logged batch: either all migrations are marked as applied or none,
	// a partial set would make migrate.FromFS replay the rest on the final schema.
	batch := session.Batch(gocql.LoggedBatch).WithContext(ctx)
	for _, name := range names[:idx+1] {
		b, err := fs.ReadFile(files, name)
		if err != nil {
			return err
		}
		sum := md5.Sum(b)
		batch.Query(insertStmt, name, hex.EncodeToString(sum[:]), len(splitStmts(b)), now, now)
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return errors.Wrap(err, "mark migrations as applied")
	}
	logger.Info(ctx, "Schema snapshot applied", "version", Version)
	return nil
}

// foreignSchema returns why the keyspace holds schema a snapshot would not create, or "" if it does not.
// Tables and types matching schema.cql are fine: left by an interrupted Apply, or restored with data
// but no migration history, where IF NOT EXISTS keeps the data and replay would fail on 014's RENAME.
func foreignSchema(ctx context.Context, session gocqlx.Session) (string, error) {
	q := session.ContextQuery(ctx, "SELECT table_name, column_name, type FROM system_schema.columns WHERE keyspace_name = ?", nil)
	iter := q.Bind(q.Keyspace()).Iter()
	got := make(map[string][]string)
	var name, col, kind string
	for iter.Scan(&name, &col, &kind) {
		got[name] = append(got[name], col+" "+kind)
	}
	if err := iter.Close(); err != nil {
		return "", errors.Wrap(err, "list columns")
	}
	delete(got, "gocqlx_migrate")
	for _, cols := range got {
		slices.Sort(cols)
	}
	if reason := diffObjects("table", snapshotTables(), got); reason != "" {
		return reason, nil
	}

	q = session.ContextQuery(ctx, "SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = ?", nil)
	iter = q.Bind(q.Keyspace()).Iter()
	got = make(map[string][]string)
	var names, kinds []string
	for iter.Scan(&name, &names, &kinds) {
		got[name] = joinFields(names, kinds)
	}
	if err := iter.Close(); err != nil {
		return "", errors.Wrap(err, "list types")
	}
	return diffObjects("type", snapshotTypes(), got), nil
}

// diffObjects returns why an existing object is not in want or differs from it, or "".
func diffObjects(what string, want, got map[string][]string) string {
	for name, fields := range got {
		w, ok := want[name]
		if !ok {
			return what + " " + name + " is not in schema.cql"
		}
		if !slices.Equal(w, fields) {
			return what + " " + name + " differs from schema.cql"
		}
	}
	return ""
}

// snapshotTables returns sorted "name type" columns of every table in schema.cql.
func snapshotTables() map[string][]string {
	out := parseFields(tableRe)
	for _, cols := range out {
		slices.Sort(cols)
	}
	return out
}

// snapshotTypes returns "name type" fields of every type in schema.cql.
func snapshotTypes() map[string][]string {
	return parseFields(typeRe)
}

// parseFields relies on schema.cql having one column or field per line.
func parseFields(re *regexp.Regexp) map[string][]string {
	out := make(map[string][]string)
	for _, m := range re.FindAllStringSubmatch(schemaCQL, -1) {
		var fields []string
		for l := range strings.SplitSeq(m[2], "\n") {
			l = strings.TrimSuffix(strings.TrimSpace(l), ",")
			if l == "" || strings.HasPrefix(l, "PRIMARY KEY") {
				continue
			}
			fields = append(fields, strings.TrimSuffix(l, " static"))
		}
		out[m[1]] = fields
	}
	return out
}

func joinFields(names, kinds []string) []string {
	out := make([]string, len(names))
	for i := range names {
		out[i] = names[i] + " " + kinds[i]
	}
	return out
}

// splitStmts splits CQL the same way gocqlx/migrate does, so that statement counts match.
func splitStmts(b []byte) []string {
	var out []string
	r := bytes.NewBuffer(b)
	for {
		stmt, err := r.ReadString(';')
		s := strings.TrimSpace(stmt)
		if err == io.EOF {
			if s != "" {
				out = append(out, s)
			}
			return out
		}
		out = append(out, s)
	}
}
