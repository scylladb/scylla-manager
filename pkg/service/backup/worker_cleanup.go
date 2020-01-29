// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/mermaid/pkg/util/parallel"
)

// Cleanup resets global stats and runs gc for each agent.
func (w *worker) Cleanup(ctx context.Context, hi []hostInfo) {
	if err := hostsInParallel(hi, parallel.NoLimit, func(h hostInfo) error {
		return w.Client.RcloneResetStats(context.Background(), h.IP)
	}); err != nil {
		w.Logger.Error(context.Background(), "Failed to reset stats", "error", err)
	}

	if err := hostsInParallel(hi, parallel.NoLimit, func(h hostInfo) error {
		return w.Client.RcloneGC(ctx, h.IP)
	}); err != nil {
		w.Logger.Error(ctx, "Failed to run GC", "error", err)
	}
}
