// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// hostInfo groups target host properties needed for backup.
type hostInfo struct {
	DC        string
	IP        string
	ID        string
	Location  Location
	RateLimit DCLimit
	Transfers int
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

	SkippedBytesOffset int64
	NewFilesSize       int64
	// willCreateVersioned is set to true when uploading the snapshot directory after
	// the deduplication results in creating versioned SSTables.
	willCreateVersioned bool
}

func (sd snapshotDir) String() string {
	return fmt.Sprintf("%s: %s/%s", sd.Host, sd.Keyspace, sd.Table)
}

// workerTools is an intersection of fields and methods
// useful for both worker and restoreWorker.
type workerTools struct {
	ClusterID   uuid.UUID
	ClusterName string
	TaskID      uuid.UUID
	RunID       uuid.UUID
	SnapshotTag string
	Config      Config
	Client      *scyllaclient.Client
	NodeConfig  map[string]configcache.NodeConfig
	Logger      log.Logger
}

// worker is responsible for coordinating backup procedure.
type worker struct {
	workerTools

	PrevStage      Stage
	Metrics        metrics.BackupMetrics
	Units          []Unit
	Schema         bytes.Buffer
	SchemaFilePath string
	OnRunProgress  func(ctx context.Context, p *RunProgress)
	// ResumeUploadProgress populates upload stats of the provided run progress
	// with previous run progress.
	// If there is no previous run there should be no update.
	// It's required to provide Size as current disk size of files.
	ResumeUploadProgress func(ctx context.Context, p *RunProgress)

	// Cache for host snapshotDirs
	snapshotDirs map[string][]snapshotDir
	mu           sync.Mutex

	memoryPool *sync.Pool

	dth deduplicateTestHooks
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
	f := func(h hostInfo) error {
		return w.Client.RcloneResetStats(context.Background(), h.IP)
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Failed to reset stats on host",
			"host", h.IP,
			"error", err,
		)
	}

	_ = hostsInParallel(hi, parallel.NoLimit, f, notify) // nolint: errcheck
}

// nodeInfo is a getter for workerTools.NodeConfig which is a workaround for #4181.
func (w *worker) nodeInfo(ctx context.Context, host string) (*scyllaclient.NodeInfo, error) {
	// Try to get direct entry in config cache
	if nc, ok := w.NodeConfig[host]; ok {
		return nc.NodeInfo, nil
	}
	// Try to get resolved entry in config cache
	if hostIP := net.ParseIP(host); hostIP != nil {
		for h, nc := range w.NodeConfig {
			if ip := net.ParseIP(h); ip != nil && hostIP.Equal(ip) {
				return nc.NodeInfo, nil
			}
		}
	}
	// Last resort - query node info from the scratch
	return w.Client.NodeInfo(ctx, host)
}
