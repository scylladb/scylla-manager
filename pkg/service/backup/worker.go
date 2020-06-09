// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"sync"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
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
	ClusterID      uuid.UUID
	ClusterName    string
	TaskID         uuid.UUID
	RunID          uuid.UUID
	SnapshotTag    string
	Config         Config
	Units          []Unit
	SchemaUploaded bool
	Client         *scyllaclient.Client
	Logger         log.Logger
	OnRunProgress  func(ctx context.Context, p *RunProgress)
	// ResumeUploadProgress populates upload stats of the provided run progress
	// with previous run progress.
	// If there is no previous run there should be no update.
	ResumeUploadProgress func(ctx context.Context, p *RunProgress)

	// Cache for host snapshotDirs
	snapshotDirs map[string][]snapshotDir
	mu           sync.Mutex

	rings      map[string]scyllaclient.Ring
	memoryPool *sync.Pool

	// Note: clusterSession may be nil.
	clusterSession gocqlx.Session
}

func (w *worker) WithLogger(logger log.Logger) *worker {
	w.Logger = logger
	return w
}

func (w *worker) hostSnapshotDirs(h hostInfo) []snapshotDir {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotDirs[h.IP]
}

func (w *worker) setSnapshotDirs(h hostInfo, dirs []snapshotDir) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.snapshotDirs == nil {
		w.snapshotDirs = make(map[string][]snapshotDir)
	}

	w.snapshotDirs[h.IP] = dirs
}

// cleanup resets global stats for each agent.
func (w *worker) cleanup(ctx context.Context, hi []hostInfo) {
	if err := hostsInParallel(hi, parallel.NoLimit, func(h hostInfo) error {
		return w.Client.RcloneResetStats(context.Background(), h.IP)
	}); err != nil {
		w.Logger.Error(ctx, "Failed to reset stats", "error", err)
	}
}
