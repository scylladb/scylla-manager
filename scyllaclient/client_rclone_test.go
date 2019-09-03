// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestRcloneCatIntegration(t *testing.T) {
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

func TestRcloneListDirIntegration(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	ctx := context.Background()

	got, err := client.RcloneListDir(ctx, testHost, "testdata/rclone/list", true)
	if err != nil {
		t.Fatal(err)
	}

	expected := []*models.ListItem{
		{
			IsDir:    false,
			MimeType: "text/plain; charset=utf-8",
			ModTime:  "2019-05-08T08:32:10.401408354+02:00",
			Name:     "file.txt",
			Path:     "file.txt",
			Size:     4,
		},
	}

	if diff := cmp.Diff(got, expected, cmpopts.IgnoreFields(models.ListItem{}, "ModTime")); diff != "" {
		t.Fatal(got, diff)
	}
}

func TestRcloneListDirNotFoundIntegration(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	ctx := context.Background()

	_, err := client.RcloneListDir(ctx, testHost, "testdata/rclone/not-found", true)
	if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
		t.Fatal("expected not found")
	}
}

func TestRcloneDiskUsageIntegration(t *testing.T) {
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
