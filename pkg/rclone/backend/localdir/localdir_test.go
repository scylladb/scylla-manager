// Copyright (C) 2017 ScyllaDB

package localdir

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"go.uber.org/multierr"
)

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
