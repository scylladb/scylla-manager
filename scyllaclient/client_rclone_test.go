// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/scylladb/mermaid/rclone/backend/data"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/models"
)

const (
	testHost   = "127.0.0.1"
	testRemote = "s3"
	testBucket = "testing"
)

func remotePath(path string) string {
	return testRemote + ":" + testBucket + path
}

func setRootDir(t *testing.T) func() {
	t.Helper()
	old := data.RootDir
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(t)
	}
	data.RootDir = dir

	return func() {
		data.RootDir = old
	}
}

func setRootDirVal(t *testing.T, root string) func() {
	t.Helper()
	old := data.RootDir
	data.RootDir = root

	return func() {
		data.RootDir = old
	}
}

func TestRcloneCatIntegration(t *testing.T) {
	defer setRootDir(t)()
	client, _, cl := newMockRcloneServer(t)
	defer cl()

	expected, err := ioutil.ReadFile("testdata/rclone/cat/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	for _, test := range []struct {
		Name  string
		Path  string
		Error bool
	}{
		{
			Name:  "file",
			Path:  "data:testdata/rclone/cat/file.txt",
			Error: false,
		},
		{
			Name:  "dir",
			Path:  "data:testdata/rclone/cat",
			Error: true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			got, err := client.RcloneCat(ctx, testHost, test.Path)
			if test.Error && err == nil {
				t.Fatal(err)
			} else if !test.Error && err != nil {
				t.Fatal(err)
			} else if err != nil {
				return
			}

			if diff := cmp.Diff(got, expected); diff != "" {
				t.Fatal(got, diff)
			}
		})
	}
}

func TestRcloneCatLimit(t *testing.T) {
	defer setRootDirVal(t, "/dev")()
	client, _, cl := newMockRcloneServer(t)
	defer cl()

	got, err := client.RcloneCat(context.Background(), testHost, "data:zero")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > rcserver.CatLimit {
		t.Errorf("Expected max red bytes to be %d, got %d", rcserver.CatLimit, len(got))
	}
}

func TestSplitRemotePath(t *testing.T) {
	table := []struct {
		Name  string
		Path  string
		Fs    string
		File  string
		Error bool
	}{
		{
			Name: "Single path",
			Path: "data:file",
			Fs:   "data:.",
			File: "file",
		},
		{
			Name: "Long path",
			Path: "data:dir/file",
			Fs:   "data:dir",
			File: "file",
		},
		{
			Name:  "Invalid file path",
			Path:  "data:",
			Error: true,
		},
		{
			Name:  "Invalid file system",
			Path:  "data",
			Error: true,
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			fs, file, err := scyllaclient.SplitRemotePath(test.Path)
			if err != nil && !test.Error {
				t.Fatal(err)
			} else if err == nil && test.Error {
				t.Fatal("Expected error")
			}
			if fs != test.Fs {
				t.Errorf("Expected fs %q, got %q", test.Fs, fs)
			}
			if file != test.File {
				t.Errorf("Expected file %q, got %q", test.File, file)
			}
		})
	}
}

func TestRcloneListDir(t *testing.T) {
	defer setRootDir(t)()
	f := func(file string, isDir bool) *models.ListItem {
		return &models.ListItem{
			Path:  file,
			Name:  path.Base(file),
			IsDir: isDir,
		}
	}
	opts := cmpopts.IgnoreFields(models.ListItem{}, "MimeType", "ModTime", "Size")

	table := []struct {
		Name     string
		Opts     *scyllaclient.RcloneListDirOpts
		Expected []*models.ListItem
	}{
		{
			Name:     "default",
			Expected: []*models.ListItem{f("file.txt", false), f("subdir", true)},
		},
		{
			Name:     "recursive",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true},
			Expected: []*models.ListItem{f("file.txt", false), f("subdir", true), f("subdir/file.txt", false)},
		},
		{
			Name:     "recursive files",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true, FilesOnly: true},
			Expected: []*models.ListItem{f("file.txt", false), f("subdir/file.txt", false)},
		},
		{
			Name:     "recursive dirs",
			Opts:     &scyllaclient.RcloneListDirOpts{Recurse: true, DirsOnly: true},
			Expected: []*models.ListItem{f("subdir", true)},
		},
	}

	client, _, cl := newMockRcloneServer(t)
	defer cl()

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			files, err := client.RcloneListDir(context.Background(), testHost, "data:testdata/rclone/list", test.Opts)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(files, test.Expected, opts); diff != "" {
				t.Fatal("RcloneListDir() diff", diff)
			}
		})
	}
}

func TestRcloneListDirNotFound(t *testing.T) {
	defer setRootDir(t)()
	client, _, cl := newMockRcloneServer(t)
	defer cl()

	ctx := context.Background()

	_, err := client.RcloneListDir(ctx, testHost, "data:testdata/rclone/not-found", nil)
	if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
		t.Fatal("expected not found")
	}
}

func TestRcloneDiskUsage(t *testing.T) {
	defer setRootDir(t)()
	client, _, cl := newMockRcloneServer(t)
	defer cl()

	ctx := context.Background()

	got, err := client.RcloneDiskUsage(ctx, testHost, "data:testdata/rclone/")
	if err != nil {
		t.Fatal(err)
	}

	if got.Total <= 0 || got.Free <= 0 || got.Used <= 0 {
		t.Errorf("Expected usage bigger than zero, got: %+v", got)
	}
}

func newMockRcloneServer(t *testing.T) (*scyllaclient.Client, string, func()) {
	t.Helper()

	rc := rcserver.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/rclone")
		rc.ServeHTTP(w, r)
	}))

	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	config := scyllaclient.DefaultConfig()
	config.Hosts = []string{host}
	config.Scheme = "http"
	config.AgentPort = port

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	return client, server.URL, func() { server.Close() }
}

func TestRcloneConfigurationIsNotAccessible(t *testing.T) {
	defer setRootDir(t)()
	_, url, cl := newMockRcloneServer(t)
	defer cl()

	values := map[string]string{}
	jsonValue, _ := json.Marshal(values)
	paths := []string{"config/create", "config/get", "config/providers", "config/delete"}

	for _, p := range paths {
		resp, err := http.Post(url+"/"+p, "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusNotFound {
			res, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(string(res))
			t.Fatalf("Expected bad request, got: %+v", resp)
		}
	}
}
