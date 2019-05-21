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

const (
	remote = "s3"
	bucket = "testing"
)

// setupS3Remote  removes testing bucket if it's already there and creates new
// bucket and then it registers it with rclone server.
// It returns remote path to the bucket.
func setupS3Remote(t *testing.T, c *scyllaclient.Client, host string) string {
	t.Helper()

	S3InitBucket(t, bucket)

	err := c.RcloneRegisterS3Remote(context.Background(), host, remote, S3ParamsFromFlags())
	if err != nil {
		t.Fatal(err)
	}
	return remote + ":" + bucket
}

func newClient(t *testing.T) (*scyllaclient.Client, string, func()) {
	t.Helper()
	rch := rcserver.New()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/rclone")
		rch.ServeHTTP(w, r)
	}))

	host, port, err := net.SplitHostPort(s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	config := scyllaclient.DefaultConfig()
	config.Hosts = []string{host}
	config.Transport = http.DefaultTransport
	config.AgentPort = port

	c, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	return c, host, func() { s.Close() }
}

func TestRcloneListDirIntegration(t *testing.T) {
	ctx := context.Background()
	c, host, finish := newClient(t)
	defer finish()

	got, err := c.RcloneListDir(ctx, host, "testdata/rclone/list", true)
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
	ctx := context.Background()
	c, host, finish := newClient(t)
	defer finish()

	got, err := c.RcloneDiskUsage(ctx, host, "testdata/rclone/")
	if err != nil {
		t.Fatal(err)
	}

	if got.Total <= 0 || got.Free <= 0 || got.Used <= 0 {
		t.Errorf("Expected usage bigger than zero, got: %+v", got)
	}
}
func TestRcloneCopyDirIntegration(t *testing.T) {
	ctx := context.Background()
	c, host, finish := newClient(t)
	defer finish()

	remotePath := setupS3Remote(t, c, host) + "/copy"

	id, err := c.RcloneCopyDir(ctx, host, remotePath, "testdata/rclone/copy")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err := c.RcloneJobStatus(ctx, host, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy dir job to finish successfully")
	}

	f, err := c.RcloneListDir(ctx, host, remotePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != 3 {
		t.Errorf("Expected bucket have 3 items, got: len(files)=%d", len(f))
	}

	id, err = c.RcloneDeleteDir(ctx, host, remotePath)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err = c.RcloneJobStatus(ctx, host, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected purge job to finish successfully")
	}

	f, err = c.RcloneListDir(ctx, host, remotePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) > 0 {
		t.Errorf("Expected bucket to be empty, got: %v", f)
	}
}

func TestRcloneCopyFileIntegration(t *testing.T) {
	ctx := context.Background()
	c, host, finish := newClient(t)
	defer finish()

	remotePath := setupS3Remote(t, c, host)

	dstPath := remotePath + "/file2.txt"

	id, err := c.RcloneCopyFile(ctx, host, dstPath, "testdata/rclone/copy/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err := c.RcloneJobStatus(ctx, host, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy file job to finish successfully")
	}

	f, err := c.RcloneListDir(ctx, host, remotePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != 1 {
		t.Errorf("Expected bucket have 1 item, got: len(files)=%d", len(f))
	}

	id, err = c.RcloneDeleteFile(ctx, host, dstPath)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	status, err = c.RcloneJobStatus(ctx, host, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected purge job to finish successfully")
	}

	f, err = c.RcloneListDir(ctx, host, remotePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(f))
	}
}
