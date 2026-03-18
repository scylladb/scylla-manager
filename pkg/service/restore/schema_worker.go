// Copyright (C) 2026 ScyllaDB

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

type schemaWorker struct {
	worker
}

func (w *schemaWorker) restore(ctx context.Context) error {
	w.run.Stage = StageData
	w.insertRun(ctx)

	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring schema")
	defer w.logger.Info(ctx, "Restoring schema finished")

	return w.restoreFromSchemaFile(ctx)
}

func (w *schemaWorker) restoreFromSchemaFile(ctx context.Context) error {
	w.logger.Info(ctx, "Apply schema CQL statements")
	start := timeutc.Now()

	// Alternator schema is independent of CQL schema, as each alternator table
	// lives in its own personal keyspace. On the other hand, CQL schema contains
	// alternator schema translated to CQL statements - those should be skipped.
	// Moreover, CQL schema contains auth and service levels, which might rely on
	// already existing alternator schema, so we need to restore alternator schema first.
	aw, err := newAlternatorSchemaWorker(w.alternatorClient, w.alternatorSchema)
	if err != nil {
		return errors.Wrap(err, "create alternator schema worker")
	}
	if err := aw.restore(ctx); err != nil {
		return errors.Wrap(err, "restore alternator schema")
	}

	var createdKs []string
	for _, row := range *w.cqlSchema {
		if row.Keyspace == "" {
			// Scylla 6.3 added roles and service levels to the output of
			// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
			// Those entities do not live in any particular keyspace, so that's how we identify them.
			// We are skipping them until we properly support their restoration.
			continue
		}
		if row.Keyspace == "system_replicated_keys" {
			// See https://github.com/scylladb/scylla-enterprise/issues/4168
			continue
		}
		sanitizedName := strings.TrimPrefix(strings.TrimSuffix(row.Name, "\""), "\"")
		if row.Type == "table" && strings.HasSuffix(sanitizedName, table.LWTStateTableSuffix) {
			// See https://github.com/scylladb/scylla-manager/issues/4732
			continue
		}
		if row.Keyspace == table.AuditKeyspace {
			// See https://github.com/scylladb/scylla-manager/issues/4748
			continue
		}
		if aw.isAlternatorSchemaRow(row) {
			continue
		}
		// Sometimes a single object might require multiple CQL statements (e.g. table with dropped and added column)
		for _, stmt := range parseCQLStatement(row.CQLStmt) {
			if err := w.clusterSession.ExecStmt(stmt); err != nil {
				if dropErr := dropKeyspaces(createdKs, w.clusterSession); dropErr != nil {
					w.logger.Error(ctx, "Couldn't rollback already applied schema changes", "error", dropErr)
				}
				return errors.Wrapf(err, "create %s (%s) with %s", row.Name, row.Keyspace, stmt)
			}
			if row.Type == "keyspace" {
				createdKs = append(createdKs, row.Name)
			}
		}
	}
	end := timeutc.Now()

	// Insert dummy run for some progress display
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			pr := RunProgress{
				ClusterID:           w.run.ClusterID,
				TaskID:              w.run.TaskID,
				RunID:               w.run.ID,
				Keyspace:            u.Keyspace,
				Table:               t.Table,
				DownloadStartedAt:   &start,
				DownloadCompletedAt: &start,
				RestoreStartedAt:    &start,
				RestoreCompletedAt:  &end,
				Downloaded:          t.Size,
				Restored:            t.Size,
			}
			w.insertRunProgress(ctx, &pr)
		}
	}

	w.logger.Info(ctx, "Restored schema from schema file")
	return nil
}

// parseCQLStatement splits composite CQL statement into a slice of single CQL statements.
func parseCQLStatement(cql string) []string {
	var out []string
	for stmt := range strings.SplitSeq(cql, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

func dropKeyspaces(keyspaces []string, session gocqlx.Session) error {
	const dropKsStmt = "DROP KEYSPACE IF EXISTS %q"
	for _, ks := range keyspaces {
		if err := session.ExecStmt(fmt.Sprintf(dropKsStmt, ks)); err != nil {
			return errors.Wrapf(err, "drop keyspace %q", ks)
		}
	}
	return nil
}
