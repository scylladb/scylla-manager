// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"strings"
	"sync"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
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
	Progress []*RunProgress
}

type worker struct {
	ClusterID     uuid.UUID
	TaskID        uuid.UUID
	RunID         uuid.UUID
	Config        Config
	Units         []Unit
	Client        *scyllaclient.Client
	Logger        log.Logger
	OnRunProgress func(ctx context.Context, p *RunProgress)

	// Cache for host snapshotDirs
	dirs   map[string][]snapshotDir
	dirsMu sync.Mutex
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

func snapshotTag(id uuid.UUID) string {
	return "sm_" + id.String()
}

func claimTag(tag string) bool {
	return strings.HasPrefix(tag, "sm_")
}
