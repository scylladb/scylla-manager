// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// dropsViews drops all Materialized View or Secondary Index which base tables are exists in the backup and returns
// what was dropped, so it can be re-created using worker.reCreateViews.
func (w *worker) dropViews(ctx context.Context, workload []hostWorkload) ([]View, error) {
	start := timeutc.Now()
	defer func() {
		w.logger.Info(ctx, "Drop views", "took", timeutc.Since(start))
	}()

	views, err := w.getViews(ctx, getTablesToRestore(workload))
	if err != nil {
		return nil, errors.Wrap(err, "get views")
	}

	for _, view := range views {
		if err := w.dropView(ctx, view); err != nil {
			return nil, errors.Wrap(err, "drop view")
		}
	}

	return views, nil
}

func (w *worker) dropView(ctx context.Context, view View) error {
	w.logger.Info(ctx, "Dropping view",
		"keyspace", view.Keyspace,
		"view", view.View,
		"type", view.Type,
	)

	op := func() error {
		dropStmt := ""
		switch view.Type {
		case SecondaryIndex:
			dropStmt = "DROP INDEX IF EXISTS %q.%q"
		case MaterializedView:
			dropStmt = "DROP MATERIALIZED VIEW IF EXISTS %q.%q"
		}

		return w.clusterSession.ExecStmt(fmt.Sprintf(dropStmt, view.Keyspace, view.View))
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Dropping view failed",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

func (w *worker) getViews(ctx context.Context, tablesToRestore map[scyllaTable]struct{}) ([]View, error) {
	keyspaces, err := w.client.Keyspaces(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "get keyspaces")
	}

	var views []View
	for _, ks := range keyspaces {
		meta, err := w.clusterSession.KeyspaceMetadata(ks)
		if err != nil {
			return nil, errors.Wrapf(err, "get keyspace %s metadata", ks)
		}

		for _, index := range meta.Indexes {
			if _, ok := tablesToRestore[scyllaTable{keyspace: index.KeyspaceName, table: index.TableName}]; !ok {
				continue
			}
			dummyMeta := gocql.KeyspaceMetadata{
				Indexes: map[string]*gocql.IndexMetadata{index.Name: index},
			}

			schema, err := dummyMeta.ToCQL()
			if err != nil {
				return nil, errors.Wrapf(err, "get index %s.%s create statement", ks, index.Name)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, SecondaryIndex)
			if err != nil {
				return nil, err
			}

			views = append(views, View{
				Keyspace:   index.KeyspaceName,
				View:       index.Name,
				Type:       SecondaryIndex,
				BaseTable:  index.TableName,
				CreateStmt: stmt,
			})
		}

		for _, view := range meta.Views {
			if _, ok := tablesToRestore[scyllaTable{keyspace: view.KeyspaceName, table: view.BaseTableName}]; !ok {
				continue
			}
			dummyMeta := gocql.KeyspaceMetadata{
				Views: map[string]*gocql.ViewMetadata{view.ViewName: view},
			}

			schema, err := dummyMeta.ToCQL()
			if err != nil {
				return nil, errors.Wrapf(err, "get view %s.%s create statement", ks, view.ViewName)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, MaterializedView)
			if err != nil {
				return nil, err
			}

			views = append(views, View{
				Keyspace:   view.KeyspaceName,
				View:       view.ViewName,
				Type:       MaterializedView,
				BaseTable:  view.BaseTableName,
				CreateStmt: stmt,
			})
		}
	}

	return views, nil
}

var (
	regexMV = regexp.MustCompile(`CREATE\s*MATERIALIZED\s*VIEW`)
	regexSI = regexp.MustCompile(`CREATE\s*INDEX`)
)

// create stmt has to contain "IF NOT EXISTS" clause as we have to be able to resume restore from any point.
func addIfNotExists(stmt string, t ViewType) (string, error) {
	var loc []int
	switch t {
	case MaterializedView:
		loc = regexMV.FindStringIndex(stmt)
	case SecondaryIndex:
		loc = regexSI.FindStringIndex(stmt)
	}
	if loc == nil {
		return "", fmt.Errorf("unknown create view statement %s", stmt)
	}

	return stmt[loc[0]:loc[1]] + " IF NOT EXISTS" + stmt[loc[1]:], nil
}

// reCreateViews re-creates views (materialized views or secondary indexes).
func (w *worker) reCreateViews(ctx context.Context, views []View) error {
	start := timeutc.Now()
	defer func() {
		w.logger.Info(ctx, "Re-create views", "took", timeutc.Since(start))
	}()
	for _, view := range views {
		if err := w.createView(ctx, view); err != nil {
			return errors.Wrap(err, "create view")
		}
		pr := w.reCreateViewProgress(ctx, view)
		if err := w.waitForViewBuilding(ctx, view, pr); err != nil {
			return errors.Wrap(err, "wait for view")
		}
	}
	return nil
}

// createView creates specified Materialized View or Secondary Index.
func (w *worker) createView(ctx context.Context, view View) error {
	w.logger.Info(ctx, "Creating view",
		"keyspace", view.Keyspace,
		"view", view.View,
		"type", view.Type,
		"statement", view.CreateStmt,
	)

	op := func() error {
		return w.clusterSession.ExecStmt(view.CreateStmt)
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Creating view failed",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

// Scylla operation might take a really long (and difficult to estimate) time.
// This func exits ONLY on: success, context cancel or error.
func (w *worker) waitForViewBuilding(ctx context.Context, view View, pr *RunViewProgress) error {
	viewTableName := view.View
	if view.Type == SecondaryIndex {
		viewTableName += "_index"
	}

	status, err := w.client.ViewBuildStatus(ctx, view.Keyspace, viewTableName)
	if err != nil {
		return err
	}

	w.updateReCreateViewProgress(ctx, pr, status)

	if status == scyllaclient.StatusUnknown || status == scyllaclient.StatusStarted {
		w.logger.Info(ctx, "Waiting for view",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"status", status,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
		return w.waitForViewBuilding(ctx, view, pr)
	}

	return nil
}
