// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/models"
)

func newMockRcloneServer(t *testing.T) (*scyllaclient.Client, func()) {
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
	return client, func() { server.Close() }
}

const (
	testHost = "127.0.0.1"
)

func TestRcloneCat(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	expected, err := ioutil.ReadFile("testdata/rclone/cat/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	for _, test := range []struct {
		Name string
		Path string
	}{
		{
			Name: "file",
			Path: "testdata/rclone/cat/file.txt",
		},
		{
			Name: "dir",
			Path: "testdata/rclone/cat",
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			got, err := client.RcloneCat(ctx, testHost, test.Path)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, expected); diff != "" {
				t.Fatal(got, diff)
			}
		})
	}
}

func TestRcloneListDir(t *testing.T) {
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

	client, close := newMockRcloneServer(t)
	defer close()

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			files, err := client.RcloneListDir(context.Background(), testHost, "testdata/rclone/list", test.Opts)
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
	client, close := newMockRcloneServer(t)
	defer close()

	ctx := context.Background()

	_, err := client.RcloneListDir(ctx, testHost, "testdata/rclone/not-found", nil)
	if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
		t.Fatal("expected not found")
	}
}

func TestRcloneDiskUsage(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	ctx := context.Background()

	got, err := client.RcloneDiskUsage(ctx, testHost, "testdata/rclone/")
	if err != nil {
		t.Fatal(err)
	}

	if got.Total <= 0 || got.Free <= 0 || got.Used <= 0 {
		t.Errorf("Expected usage bigger than zero, got: %+v", got)
	}
}
