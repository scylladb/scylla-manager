// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
)

// ClearAction represents removal of all files in Dir.
type ClearAction struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	Dir      string `json:"dir"`
}

// DownloadAction represents download of files to Dir.
type DownloadAction struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	Size     int64  `json:"size"`
	Dir      string `json:"dir"`
}

// Plan specifies actions taken by downloader.
type Plan struct {
	ClearActions    []ClearAction    `json:"clear_actions"`
	DownloadActions []DownloadAction `json:"download_actions"`
	// BaseDir can be set externally to set prefix to paths printed in WriteTo.
	BaseDir string `json:"-"`

	m backup.ManifestInfoWithContent `json:"-"`
}

func (p Plan) WriteTo(w io.Writer) (n int64, err error) {
	b := &strings.Builder{}
	defer func() {
		if err == nil {
			m, e := w.Write([]byte(b.String()))
			n, err = int64(m), e
		}
	}()

	if p.m.ClusterName == "" {
		fmt.Fprintf(b, "Cluster:\t%s\n", p.m.ClusterID)
	} else {
		fmt.Fprintf(b, "Cluster:\t%s (%s)\n", p.m.ClusterName, p.m.ClusterID)
	}
	fmt.Fprintf(b, "Datacenter:\t%s\n", p.m.DC)
	if p.m.IP == "" {
		fmt.Fprintf(b, "Node:\t\t%s\n", p.m.NodeID)
	} else {
		fmt.Fprintf(b, "Node:\t\t%s (%s)\n", p.m.IP, p.m.NodeID)
	}
	t, err := backup.SnapshotTagTime(p.m.SnapshotTag)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(b, "Time:\t\t%s\n", t)
	var size int64
	for _, a := range p.DownloadActions {
		size += a.Size
	}
	fmt.Fprintf(b, "Size:\t\t%s\n", fs.SizeSuffix(size))

	absPath := func(dir string) string {
		if p.BaseDir == "" {
			return dir
		}

		dir = path.Join(p.BaseDir, dir)
		if abs, err := filepath.Abs(dir); err == nil {
			dir = abs
		}
		return dir
	}

	if len(p.ClearActions) > 0 {
		fmt.Fprintln(b)
		fmt.Fprintln(b, "Clear:")
		for _, a := range p.ClearActions {
			fmt.Fprintf(b, "  - %s.%s at %s\n", a.Keyspace, a.Table, absPath(a.Dir))
		}
	}
	if len(p.DownloadActions) > 0 {
		fmt.Fprintln(b)
		fmt.Fprintln(b, "Download:")
		for _, a := range p.DownloadActions {
			fmt.Fprintf(b, "  - %s.%s (%s) to %s\n", a.Keyspace, a.Table, fs.SizeSuffix(a.Size), absPath(a.Dir))
		}
	}

	return
}
