// Copyright (C) 2017 ScyllaDB

package localdir

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/rc"
	"go.uber.org/multierr"
)

func TestNewFsJailContainment(t *testing.T) {
	jailDir := t.TempDir()

	// Create a sibling directory that shares the byte prefix with jailDir.
	siblingDir := jailDir + "-sibling"
	if err := os.MkdirAll(siblingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(siblingDir)
	if err := os.WriteFile(filepath.Join(siblingDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a legitimate subdirectory inside the jail.
	legitDir := filepath.Join(jailDir, "legit")
	if err := os.MkdirAll(legitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legitDir, "visible.txt"), []byte("visible"), 0o644); err != nil {
		t.Fatal(err)
	}

	newFs := NewFs(jailDir)
	ctx := context.Background()

	tests := []struct {
		name        string
		root        string
		expectFiles []string // files we expect to find at "/"
		denyFiles   []string // files that must NOT appear
	}{
		{
			name:        "legitimate subdirectory is accessible",
			root:        "legit",
			expectFiles: []string{"visible.txt"},
		},
		{
			name:        "absolute path inside jail is allowed",
			root:        filepath.Join(jailDir, "legit"),
			expectFiles: []string{"visible.txt"},
		},
		{
			name:      "prefix-collision sibling is jailed",
			root:      siblingDir,
			denyFiles: []string{"secret.txt"},
		},
		{
			name:      "absolute path outside jail is rewritten under jail",
			root:      "/etc",
			denyFiles: []string{"passwd", "hosts"},
		},
		{
			name:      "dot-dot traversal is rejected",
			root:      "../../../etc",
			denyFiles: []string{"passwd", "hosts"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := newFs(ctx, "test", tc.root, configmap.New())
			if err != nil {
				// ErrorObjectNotFound means the path was rejected or doesn't exist — that's safe.
				if errors.Is(err, fs.ErrorObjectNotFound) {
					return
				}
				t.Fatal(err)
			}
			entries, err := f.List(ctx, "")
			if err != nil {
				// Directory doesn't exist inside jail — that's fine, path was rewritten.
				return
			}

			found := make(map[string]bool)
			for _, e := range entries {
				found[e.Remote()] = true
			}

			for _, want := range tc.expectFiles {
				if !found[want] {
					t.Errorf("expected file %q not found in listing", want)
				}
			}
			for _, deny := range tc.denyFiles {
				if found[deny] {
					t.Errorf("SECURITY VIOLATION: file %q from outside jail is visible", deny)
				}
			}
		})
	}
}

func TestNewFsWithGlobalCache(t *testing.T) {
	// Skipping this test because it's only possible to test it manually after
	// commenting out expire init to affect global cache:
	// New() *Cache in vendor/github.com/rclone/rclone/lib/cache/cache.go:25
	// Cache is defined as global value in:
	// c = cache.New() vendor/github.com/rclone/rclone/fs/cache/cache.go:14
	t.Skip("Skipping because manual test")
	const providerName = "fscache"
	p, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	Init(providerName, "testing", p)
	errs := multierr.Combine(
		fs.ConfigFileSet(providerName, "type", providerName),
		fs.ConfigFileSet(providerName, "disable_checksum", "true"),
	)
	if errs != nil {
		t.Fatal(errs)
	}
	ctx := context.Background()
	f, err := rc.GetFs(ctx, map[string]interface{}{"fs": providerName + ":"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Features().About(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, err = rc.GetFs(ctx, map[string]interface{}{"fs": providerName + ":"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Features().About(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, err = rc.GetFs(ctx, map[string]interface{}{"fs": providerName + ":subdir"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Features().About(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
