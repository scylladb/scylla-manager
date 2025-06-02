// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// dropsViews drops all Materialized View or Secondary Index which base tables are exists in the backup and returns
// what was dropped, so it can be re-created using worker.reCreateViews.
func (w *worker) dropViews(ctx context.Context, workload []hostWorkload) ([]View, error) {
	start := timeutc.Now()
	defer func() {
		w.logger.Info(ctx, "Drop views", "took", timeutc.Since(start))
	}()

	views, err := w.getViews(ctx, workload)
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

func (w *worker) getViews(ctx context.Context, workload []hostWorkload) ([]View, error) {
	tablesToRestore := getTablesToRestore(workload)
	views, err := w.viewsFromSchema(ctx, workload)
	if err != nil {
		return nil, errors.Wrap(err, "get views from schema")
	}
	result := make([]View, 0, len(views))
	for _, view := range views {
		if _, ok := tablesToRestore[scyllaTable{keyspace: view.Keyspace, table: view.BaseTable}]; !ok {
			continue
		}
		result = append(result, view)
	}

	return result, nil
}

func (w *worker) getBaseTableName(keyspace, name string) (string, error) {
	q := qb.Select("system_schema.views").
		Columns("base_table_name").
		Where(qb.Eq("keyspace_name"), qb.Eq("view_name")).
		Query(w.clusterSession).
		Bind(keyspace, name)
	defer q.Release()
	var baseTableName string
	err := q.Scan(&baseTableName)
	if err != nil {
		return "", errors.Wrapf(err, "scan base table name for view %s.%s", keyspace, name)
	}
	return baseTableName, nil
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
	status, err := w.client.ViewBuildStatus(ctx, view.Keyspace, view.View)
	if err != nil {
		return err
	}

	w.updateReCreateViewProgress(ctx, pr, status)
	switch status {
	case scyllaclient.StatusUnknown:
		w.metrics.SetViewBuildStatus(w.runInfo.ClusterID, view.Keyspace, view.View, metrics.BuildStatusUnknown)
	case scyllaclient.StatusStarted:
		w.metrics.SetViewBuildStatus(w.runInfo.ClusterID, view.Keyspace, view.View, metrics.BuildStatusStarted)
	case scyllaclient.StatusSuccess:
		w.metrics.SetViewBuildStatus(w.runInfo.ClusterID, view.Keyspace, view.View, metrics.BuildStatusSuccess)
		return nil
	}

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

func (w *worker) viewsFromSchema(ctx context.Context, workload []hostWorkload) ([]View, error) {
	// In order to ensure schema consistency, we need to call raft read barrier on a host
	// from which we are going to read the schema.
	host := workload[0].host
	if host.SafeDescribeMethod != scyllaclient.SafeDescribeMethodReadBarrierAPI {
		// Let's try to find the host that supports raft read barrier api.
		for _, w := range workload {
			if w.host.SafeDescribeMethod == scyllaclient.SafeDescribeMethodReadBarrierAPI {
				host = w.host
				break
			}
		}
	}

	hostSession, err := w.singleHostCQLSession(ctx, w.runInfo.ClusterID, host.Addr)
	if err != nil {
		return nil, errors.Wrap(err, "single host cql session")
	}

	if err := w.raftReadBarrier(ctx, hostSession, host); err != nil {
		return nil, errors.Wrap(err, "raft read barrier")
	}

	describedSchema, err := query.DescribeSchemaWithInternals(hostSession)
	if err != nil {
		return nil, errors.Wrap(err, "describe schema")
	}
	var result []View
	for _, stmt := range describedSchema {
		if stmt.Keyspace == "" {
			continue
		}
		if !slices.Contains([]string{"view", "index"}, stmt.Type) {
			continue
		}

		viewType := MaterializedView
		if stmt.Type == "index" {
			viewType = SecondaryIndex
		}
		baseTableName, err := w.getBaseTableName(stmt.Keyspace, stmt.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "get base table name for view %s.%s", stmt.Keyspace, stmt.Name)
		}
		createStmt, err := addIfNotExists(stmt.CQLStmt, viewType)
		if err != nil {
			return nil, err
		}
		result = append(result, View{
			Keyspace:    stmt.Keyspace,
			View:        stmt.Name,
			Type:        viewType,
			BaseTable:   baseTableName,
			CreateStmt:  createStmt,
			BuildStatus: scyllaclient.StatusUnknown, // We don't know the build status yet.
		})
	}
	return result, nil
}
