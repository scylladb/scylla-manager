// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"sync"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// hostInfo groups target host properties needed for backup.
type hostInfo struct {
	DC        string
	IP        string
	ID        string
	Location  Location
	RateLimit DCLimit
}

func (h hostInfo) String() string {
	return h.IP
}

// snapshotDir represents a remote directory containing a table snapshot.
type snapshotDir struct {
	Host     string
	Unit     int64
	Path     string
	Keyspace string
	Table    string
	Version  string
	Progress *RunProgress
}

func (sd snapshotDir) String() string {
	return fmt.Sprintf("%s: %s/%s", sd.Host, sd.Keyspace, sd.Table)
}

type worker struct {
	ClusterID     uuid.UUID
	ClusterName   string
	TaskID        uuid.UUID
	RunID         uuid.UUID
	SnapshotTag   string
	Config        Config
	Units         []Unit
	Client        *scyllaclient.Client
	Logger        log.Logger
	OnRunProgress func(ctx context.Context, p *RunProgress)

	// Cache for host snapshotDirs
	dirs   map[string][]snapshotDir
	dirsMu sync.Mutex
}

func (w *worker) WithLogger(logger log.Logger) *worker {
	w.Logger = logger
	return w
}

func (w *worker) hostSnapshotDirs(h hostInfo) []snapshotDir {
	w.dirsMu.Lock()
	defer w.dirsMu.Unlock()
	return w.dirs[h.IP]
}

func (w *worker) setHostSnapshotDirs(h hostInfo, dirs []snapshotDir) {
	w.dirsMu.Lock()
	defer w.dirsMu.Unlock()
	if w.dirs == nil {
		w.dirs = make(map[string][]snapshotDir)
	}

	w.dirs[h.IP] = dirs
}

// Cleanup resets global stats and runs gc for each agent.
func (w *worker) Cleanup(ctx context.Context, hi []hostInfo) {
	if err := hostsInParallel(hi, parallel.NoLimit, func(h hostInfo) error {
		return w.Client.RcloneResetStats(ctx, h.IP)
	}); err != nil {
		w.Logger.Error(ctx, "Failed to reset stats", "error", err)
	}
}
