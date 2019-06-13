// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	. "github.com/scylladb/mermaid/mermaidtest"
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
	testRemote = "s3"
	testBucket = "testing"
	testHost   = "127.0.0.1"
)

func registerRemote(t *testing.T, c *scyllaclient.Client, host string) {
	t.Helper()

	if err := c.RcloneRegisterS3Remote(context.Background(), host, testRemote, NewS3Params()); err != nil {
		t.Fatal(err)
	}
}

func remotePath(path string) string {
	return testRemote + ":" + testBucket + path
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
func TestRcloneCopyDirIntegration(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	S3InitBucket(t, testBucket)
	registerRemote(t, client, testHost)

	ctx := context.Background()

	id, err := client.RcloneCopyDir(ctx, testHost, remotePath("/copy"), "testdata/rclone/copy")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err := client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy dir job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, testHost, remotePath("/copy"), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 3 {
		t.Errorf("Expected bucket have 3 items, got: len(files)=%d", len(d))
	}

	id, err = client.RcloneDeleteDir(ctx, testHost, remotePath("/copy"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err = client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected purge job to finish successfully")
	}

	d, err = client.RcloneListDir(ctx, testHost, remotePath("/copy"), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: %v", d)
	}
}

func TestRcloneCopyFileIntegration(t *testing.T) {
	client, close := newMockRcloneServer(t)
	defer close()

	S3InitBucket(t, testBucket)
	registerRemote(t, client, testHost)

	ctx := context.Background()

	id, err := client.RcloneCopyFile(ctx, testHost, remotePath("/file2.txt"), "testdata/rclone/copy/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err := client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy file job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, testHost, remotePath(""), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 1 {
		t.Errorf("Expected bucket have 1 item, got: len(files)=%d", len(d))
	}

	id, err = client.RcloneDeleteFile(ctx, testHost, remotePath("/file2.txt"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err = client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected purge job to finish successfully")
	}

	d, err = client.RcloneListDir(ctx, testHost, remotePath(""), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(d))
	}
}
