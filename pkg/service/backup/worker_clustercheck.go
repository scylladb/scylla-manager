// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
)

// CheckCluster checks the current load on every element of hosts and returns the first one exceeding the configured
// maximum threshold. See [Config.MaxLoadThreshold].
//
// If [Config.MaxLoadThreshold] was set to 100 the method is no-op.
func (w *worker) CheckCluster(ctx context.Context, hosts []hostInfo, clusterCheckBackoff retry.Backoff) error {
	if w.Config.MaxLoadThreshold >= 100 {
		return nil
	}
	notify := func(err error, wait time.Duration) {
		w.Logger.Info(ctx, "Current load of one of the nodes is above the configured threshold...", "error", err, "wait", wait)
	}

	return retry.WithNotify(ctx, func() error {
		for _, h := range hosts {
			load, err := w.Client.CurrentLoad(ctx, h.IP)
			if err != nil {
				w.Logger.Info(ctx, "Cannot check node's load", "error", err)
				return retry.Permanent(service.ErrNodeLoadMetricUnavailable)
			}
			if load >= w.Config.MaxLoadThreshold {
				return errors.Errorf("load %.2f on %s node is above configured threshold equal to %.2f",
					load, h.IP, w.Config.MaxLoadThreshold)
			}
		}
		return nil
	}, clusterCheckBackoff, notify)
}
