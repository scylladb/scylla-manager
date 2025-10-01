// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"golang.org/x/sync/errgroup"
)

func (w *worker) stageDropViews(ctx context.Context) error {
	nc, ok := w.anyNodeConfig()
	if !ok {
		return errors.New("no node config")
	}
	aw, err := newAlternatorDropViewsWorker(ctx, w.alternatorClient, nc.NodeInfo, w.run.Views)
	if err != nil {
		return errors.Wrap(err, "create alternator drop views worker")
	}
	if err := aw.dropViews(ctx); err != nil {
		return err
	}

	for _, v := range w.run.Views {
		if aw.isAlternatorView(v) {
			continue
		}

		if err := w.DropView(ctx, v); err != nil {
			return errors.Wrapf(err, "drop %s.%s", v.Keyspace, v.View)
		}
	}
	return nil
}

func (w *worker) stageRecreateViews(ctx context.Context) error {
	nc, ok := w.anyNodeConfig()
	if !ok {
		return errors.New("no node config")
	}
	aw, err := newAlternatorCreateViewsWorker(ctx, w.alternatorClient, nc.NodeInfo, w.run.Views)
	if err != nil {
		return errors.Wrap(err, "create alternator create views worker")
	}
	if err := aw.createViews(ctx); err != nil {
		return err
	}

	for _, v := range w.run.Views {
		if aw.isAlternatorView(v) {
			continue
		}

		if err := w.CreateView(ctx, v); err != nil {
			return errors.Wrapf(err, "recreate %s.%s with statement %s", v.Keyspace, v.View, v.CreateStmt)
		}
	}
	// Since we are waiting for view building with scylla rest api,
	// we can do it in the same way for cql and alternator views.
	eg, egCtx := errgroup.WithContext(ctx)
	for i := range w.run.Views {
		eg.Go(func() error {
			return w.WaitForViewBuilding(egCtx, &w.run.Views[i])
		})
	}
	return eg.Wait()
}

// DropView drops specified Materialized View or Secondary Index.
func (w *worker) DropView(ctx context.Context, view View) error {
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

// CreateView creates specified Materialized View or Secondary Index.
func (w *worker) CreateView(ctx context.Context, view View) error {
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

func (w *worker) WaitForViewBuilding(ctx context.Context, view *View) error {
	labels := metrics.RestoreViewBuildStatusLabels{
		ClusterID: w.run.ClusterID.String(),
		Keyspace:  view.Keyspace,
		View:      view.View,
	}

	viewTableName := view.View
	if view.Type == SecondaryIndex {
		viewTableName += "_index"
	}

	const retryInterval = 10 * time.Second
	for {
		status, err := w.client.ViewBuildStatus(ctx, view.Keyspace, viewTableName)
		if err != nil {
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusError)
			return err
		}

		view.BuildStatus = status
		w.insertRun(ctx)
		switch status {
		case scyllaclient.StatusUnknown:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusUnknown)
		case scyllaclient.StatusStarted:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusStarted)
		case scyllaclient.StatusSuccess:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusSuccess)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}
