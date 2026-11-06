// Copyright (C) 2026 ScyllaDB

package restore

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scylladb/go-log"

	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestCleanUploadDirIgnoresVanishedFile(t *testing.T) {
	dir := t.TempDir()
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("cleanupload", "", dir)
	if err := os.WriteFile(filepath.Join(dir, "ghost"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Remove the listed file right before agent's deletefile, as if it vanished concurrently.
	vanish := func(r *http.Request) string {
		if strings.HasSuffix(r.URL.Path, "/operations/deletefile") {
			_ = os.Remove(filepath.Join(dir, "ghost"))
		}
		return ""
	}
	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t, vanish)
	defer closeServer()

	w := &worker{client: client, logger: log.NopLogger}
	if err := w.cleanUploadDir(t.Context(), scyllaclienttest.TestHost, "cleanupload:", nil); err != nil {
		t.Fatalf("cleanUploadDir() error %s, expected missing file to be ignored", err)
	}
}
