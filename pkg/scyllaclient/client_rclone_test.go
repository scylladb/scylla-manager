// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

func TestRcloneSplitRemotePath(t *testing.T) {
	t.Parallel()

	table := []struct {
		Input  string
		Fs     string
		Remote string
		Error  bool
	}{
		{
			Input:  "rclonetest:bucket",
			Fs:     "rclonetest:bucket",
			Remote: "",
		},
		{
			Input:  "rclonetest:dir/subdir",
			Fs:     "rclonetest:dir",
			Remote: "subdir",
		},
		{
			Input:  "rclonetest:dir/subdir/subdir/subdir",
			Fs:     "rclonetest:dir",
			Remote: "subdir/subdir/subdir",
		},
		{
			Input:  "rclonetest:",
			Fs:     "rclonetest:",
			Remote: "",
		},
		{
			Error: true,
		},
		{
			Input: "data",
			Error: true,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Input, func(t *testing.T) {
			fs, file, err := scyllaclient.RcloneSplitRemotePath(test.Input)
			if err != nil && !test.Error {
				t.Fatal(err)
			} else if err == nil && test.Error {
				t.Fatal("Expected error")
			}
			if fs != test.Fs {
				t.Errorf("Expected fs %q, got %q", test.Fs, fs)
			}
			if file != test.Remote {
				t.Errorf("Expected dir path %q, got %q", test.Remote, file)
			}
		})
	}
}

func TestRcloneCat(t *testing.T) {
	t.Parallel()

	b, err := os.ReadFile("testdata/rclone/cat/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	table := []struct {
		Name   string
		Path   string
		Golden []byte
		Error  string
	}{
		{
			Name:   "meta file",
			Path:   "rclonetest:cat/backup/meta/file.txt",
			Golden: b,
		},
		{
			Name:  "other file",
			Path:  "rclonetest:cat/file.txt",
			Error: "agent [HTTP 500] permission denied",
		},
		{
			Name:  "check escape",
			Path:  "rclonetest:cat/backup/meta/../../file.txt",
			Error: "agent [HTTP 500] permission denied",
		},
	}

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			content, err := client.RcloneCat(context.Background(), scyllaclienttest.TestHost, test.Path)
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}
			if !strings.Contains(errMsg, test.Error) {
				t.Fatalf("RcloneCat() error %s expected %s", errMsg, test.Error)
			}
			if diff := cmp.Diff(content, test.Golden); diff != "" {
				t.Fatal(content, diff)
			}
		})
	}
}

func rcloneListDirIterAppendFunc(files *[]*scyllaclient.RcloneListDirItem) func(item *scyllaclient.RcloneListDirItem) {
	return func(item *scyllaclient.RcloneListDirItem) {
		i := *item
		*files = append(*files, &i)
	}
}

func TestRcloneListDir(t *testing.T) {
	t.Parallel()

	f := func(file string, isDir bool) *scyllaclient.RcloneListDirItem {
		return &scyllaclient.RcloneListDirItem{
			Path:  file,
			Name:  path.Base(file),
			IsDir: isDir,
		}
	}
	opts := cmpopts.IgnoreFields(scyllaclient.RcloneListDirItem{}, "MimeType", "ModTime", "Size")

	table := []struct {
		Name     string
		Opts     *scyllaclient.RcloneListDirOpts
		Expected []*scyllaclient.RcloneListDirItem
	}{
		{
			Name:     "default",
			Expected: []*scyllaclient.RcloneListDirItem{f("file.txt", false), f("subdir", true)},
		},
		{
			Name:     "recursive",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true},
			Expected: []*scyllaclient.RcloneListDirItem{f("file.txt", false), f("subdir", true), f("subdir/file.txt", false)},
		},
		{
			Name:     "recursive files",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true, FilesOnly: true},
			Expected: []*scyllaclient.RcloneListDirItem{f("file.txt", false), f("subdir/file.txt", false)},
		},
		{
			Name:     "recursive dirs",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true, DirsOnly: true},
			Expected: []*scyllaclient.RcloneListDirItem{f("subdir", true)},
		},
	}

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	t.Run("group", func(t *testing.T) {
		for i := range table {
			test := table[i]

			t.Run(test.Name, func(t *testing.T) {
				t.Parallel()

				check := func(t *testing.T, files []*scyllaclient.RcloneListDirItem, err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(files, test.Expected, opts); diff != "" {
						t.Fatal("diff", diff)
					}
				}

				t.Run("default", func(t *testing.T) {
					files, err := client.RcloneListDir(context.Background(), scyllaclienttest.TestHost, "rclonetest:list", test.Opts)
					check(t, files, err)
				})
				t.Run("iter", func(t *testing.T) {
					var files []*scyllaclient.RcloneListDirItem
					err := client.RcloneListDirIter(context.Background(), scyllaclienttest.TestHost, "rclonetest:list", test.Opts, rcloneListDirIterAppendFunc(&files))
					check(t, files, err)
				})
			})
		}
	})
}

func TestRcloneListDirNotFound(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()
	ctx := context.Background()

	check := func(t *testing.T, err error) {
		t.Helper()
		if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
			t.Fatal("expected not found")
		}
	}

	t.Run("default", func(t *testing.T) {
		_, err := client.RcloneListDir(ctx, scyllaclienttest.TestHost, "rclonetest:not-found", nil)
		check(t, err)
	})
	t.Run("iter", func(t *testing.T) {
		err := client.RcloneListDirIter(ctx, scyllaclienttest.TestHost, "rclonetest:not-found", nil, func(_ *scyllaclient.RcloneListDirItem) {})
		check(t, err)
	})
}

func TestRcloneListDirPermissionDenied(t *testing.T) {
	t.Skip("Temporary disabled due to #1477")
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t, scyllaclienttest.PathFileMatcher("/agent/rclone/core/stats", "testdata/rclone/stats/permission_denied_error.json"))
	defer closeServer()
	ctx := context.Background()

	check := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || strings.Contains(err.Error(), "permission denied") {
			t.Fatal("expected error about permission denied, got", err)
		}
	}

	t.Run("default", func(t *testing.T) {
		_, err := client.RcloneListDir(ctx, scyllaclienttest.TestHost, "rclonetest:list", nil)
		check(t, err)
	})
	t.Run("iter", func(t *testing.T) {
		err := client.RcloneListDirIter(ctx, scyllaclienttest.TestHost, "rclonetest:list", nil, func(_ *scyllaclient.RcloneListDirItem) {})
		check(t, err)
	})
}

func TestRcloneListDirEscapeJail(t *testing.T) {
	t.Parallel()

	f := func(file string, isDir bool) *scyllaclient.RcloneListDirItem {
		return &scyllaclient.RcloneListDirItem{
			Path:  file,
			Name:  path.Base(file),
			IsDir: isDir,
		}
	}
	opts := cmpopts.IgnoreFields(scyllaclient.RcloneListDirItem{}, "MimeType", "ModTime", "Size")

	table := []struct {
		Name     string
		Opts     *scyllaclient.RcloneListDirOpts
		Path     string
		Expected []*scyllaclient.RcloneListDirItem
		Error    bool
	}{
		{
			Name:     "list subdir 1",
			Path:     "rclonejail:subdir1",
			Expected: []*scyllaclient.RcloneListDirItem{f("foo.txt", false), f("subdir2", true)},
			Error:    false,
		},
		{
			Name: "list subdir 1 recursive",
			Opts: &scyllaclient.RcloneListDirOpts{
				Recurse: true,
			},
			Path:     "rclonejail:subdir1",
			Expected: []*scyllaclient.RcloneListDirItem{f("foo.txt", false), f("subdir2", true), f("subdir2/file.txt", false)},
			Error:    false,
		},
		{
			Name:     "list just root",
			Path:     "rclonejail:/",
			Expected: []*scyllaclient.RcloneListDirItem{f("subdir1", true)},
			Error:    false,
		},
		{
			Name:     "access one level above root",
			Path:     "rclonejail:subdir1/../..",
			Expected: nil,
			Error:    true,
		},
		{
			Name:     "access several levels above root",
			Path:     "rclonejail:subdir1/../../.././...",
			Expected: nil,
			Error:    true,
		},
		{
			Name:     "access root directory",
			Path:     "rclonejail:.",
			Expected: []*scyllaclient.RcloneListDirItem{f("subdir1", true)},
			Error:    false,
		},
	}

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	t.Run("group", func(t *testing.T) {
		for i := range table {
			test := table[i]

			t.Run(test.Name, func(t *testing.T) {
				t.Parallel()

				check := func(t *testing.T, files []*scyllaclient.RcloneListDirItem, err error) {
					t.Helper()
					if test.Error && err == nil {
						for _, f := range files {
							t.Log(f)
						}
						t.Fatal("Expected error")
					} else if !test.Error && err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(files, test.Expected, opts); diff != "" {
						t.Fatal("diff", diff)
					}
				}

				t.Run("default", func(t *testing.T) {
					files, err := client.RcloneListDir(context.Background(), scyllaclienttest.TestHost, test.Path, test.Opts)
					check(t, files, err)
				})
				t.Run("iter", func(t *testing.T) {
					var files []*scyllaclient.RcloneListDirItem
					err := client.RcloneListDirIter(context.Background(), scyllaclienttest.TestHost, test.Path, test.Opts, rcloneListDirIterAppendFunc(&files))
					check(t, files, err)
				})
			})
		}
	})
}

func TestRcloneListDirIterCancelContext(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()
	ctx, cancel := context.WithCancel(context.Background())

	var files []scyllaclient.RcloneListDirItem
	f := func(item *scyllaclient.RcloneListDirItem) {
		files = append(files, *item)
		cancel()
	}

	err := client.RcloneListDirIter(ctx, scyllaclienttest.TestHost, "rclonetest:list", nil, f)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RcloneListDirIter() error %s, expected context cancelation", err)
	}
	if len(files) != 1 {
		t.Fatalf("Files = %+v, expected one item", files)
	}
}

func TestRcloneDiskUsage(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	got, err := client.RcloneDiskUsage(ctx, scyllaclienttest.TestHost, "rclonetest:")
	if err != nil {
		t.Fatal(err)
	}

	if got.Total <= 0 || got.Free <= 0 || got.Used <= 0 {
		t.Errorf("Expected usage bigger than zero, got: %+v", got)
	}

	got, err = client.RcloneDiskUsage(ctx, scyllaclienttest.TestHost, "rclonetest:")
	if err != nil {
		t.Fatal(err)
	}

	if got.Total <= 0 || got.Free <= 0 || got.Used <= 0 {
		t.Errorf("Expected usage bigger than zero, got: %+v", got)
	}
}

func TestRcloneFileInfo(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	info, err := client.RcloneFileInfo(ctx, scyllaclienttest.TestHost, "rclonetest:fileinfo/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info.Size == 0 {
		t.Errorf("RcloneFileInfo()=%+v, expected size > 0", *info)
	}
	if modTime := time.Time(info.ModTime); modTime.IsZero() {
		t.Errorf("RcloneFileInfo()=%+v, expected modTime > 0", *info)
	}
}

func TestRcloneMoveFile(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	// Put "a"
	if err := client.RclonePut(ctx, scyllaclienttest.TestHost, "tmp:move/a", bytes.NewBufferString("a")); err != nil {
		t.Fatal("RclonePut() error", err)
	}
	// Move "b"
	if err := client.RcloneMoveFile(ctx, scyllaclienttest.TestHost, "tmp:move/b", "tmp:move/a"); err != nil {
		t.Fatal("RcloneMoveFile() error", err, rth.tmpDir)
	}
	// Assert "b" exits
	if _, err := os.Stat(path.Join(rth.tmpDir, "move/b")); err != nil {
		t.Error("File b should exist", err)
	}
	// Assert "a" does not exits
	if _, err := os.Stat(path.Join(rth.tmpDir, "move/a")); !os.IsNotExist(err) {
		t.Error("File a should not exist", err)
	}

	// Try move not existing
	err := client.RcloneMoveFile(ctx, scyllaclienttest.TestHost, "tmp:move/d", "tmp:move/c")
	if err == nil || scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("RcloneMoveFile() error %s, expected 404", err)
	}
}

func TestRclonePut(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	putString := func(s string) error {
		b := bytes.NewBufferString(s)
		err := client.RclonePut(ctx, scyllaclienttest.TestHost, "tmp:put/a", b)
		if err != nil {
			t.Logf("RclonePut(%s) error = %s", s, err)
		}
		return err
	}

	// New file
	if err := putString("hello"); err != nil {
		t.Fatal(err)
	}

	// Same size file is ignored
	if err := putString("olleh"); err != nil {
		t.Fatal(err)
	}

	// Validate content
	content, err := os.ReadFile(path.Join(rth.tmpDir, "put/a"))
	if err != nil {
		t.Fatal(err)
	}
	if cmp.Diff("hello", string(content)) != "" {
		t.Fatalf("put/a = %s, expected %s", string(content), "hello")
	}
}

func TestRcloneListDirTimeouts(t *testing.T) {
	s := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"list":[`)
		b, err := json.Marshal(models.ListItem{
			IsDir:   false,
			ModTime: strfmt.DateTime{},
			Name:    "foo",
			Path:    "/bar/foo",
			Size:    42,
		})
		if err != nil {
			panic(err)
		}

		// Write items with progressively larger sleeps so that
		// RcloneListDirIter times out after 4.
		max := 100 * time.Millisecond
		for s := 10 * time.Millisecond; s < max; s *= 2 {
			w.Write(b)
			w.Write([]byte(","))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(s)
		}

		w.Write(b)
		w.Write([]byte("]}"))
	})

	host, port, closeServer := scyllaclienttest.MakeServer(t, s)
	defer closeServer()

	t.Run("default", func(t *testing.T) {
		client := scyllaclienttest.MakeClient(t, host, port, func(config *scyllaclient.Config) {
			config.Timeout = 100 * time.Millisecond
		})

		l, err := client.RcloneListDir(context.Background(), host, "s3:foo", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("got %d items", len(l))
	})

	t.Run("iter", func(t *testing.T) {
		client := scyllaclienttest.MakeClient(t, host, port, func(c *scyllaclient.Config) {
			c.ListTimeout = time.Millisecond * 60
			c.Timeout = time.Millisecond * 10
		})
		defer closeServer()

		var files []*scyllaclient.RcloneListDirItem
		err := client.RcloneListDirIter(context.Background(), scyllaclienttest.TestHost, "rclonetest:list", &scyllaclient.RcloneListDirOpts{}, rcloneListDirIterAppendFunc(&files))
		if err == nil || err.Error() != "rclone list dir timeout" {
			t.Fatal("Expected timeout")
		}
		if len(files) != 4 {
			t.Fatalf("Expected 3 files, got %d", len(files))
		}
	})
}
