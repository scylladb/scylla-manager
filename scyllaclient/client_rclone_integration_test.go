// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"testing"
	"time"

	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

const (
	testRemote = "s3"
	testBucket = "testing"
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

var listRecursively = &scyllaclient.RcloneListDirOpts{Recurse: true}

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

	res, err := client.RcloneTransferred(ctx, testHost, scyllaclient.RcloneDefaultGroup(id))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Errorf("Expected two transfers, got: len(Transferred)=%d", len(res))
	}
	for _, r := range res {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	status, err := client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy dir job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, testHost, remotePath("/copy"), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 3 {
		t.Errorf("Expected bucket have 3 items, got: len(files)=%d", len(d))
	}

	if err = client.RcloneDeleteDir(ctx, testHost, remotePath("/copy")); err != nil {
		t.Fatal(err)
	}

	d, err = client.RcloneListDir(ctx, testHost, remotePath("/copy"), listRecursively)
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

	res, err := client.RcloneTransferred(ctx, testHost, scyllaclient.RcloneDefaultGroup(id))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Errorf("Expected one transfer, got: len(Transferred)=%d", len(res))
	}
	for _, r := range res {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	status, err := client.RcloneJobStatus(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy file job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, testHost, remotePath(""), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 1 {
		t.Errorf("Expected bucket have 1 item, got: len(files)=%d", len(d))
	}

	if err := client.RcloneDeleteFile(ctx, testHost, remotePath("/file2.txt")); err != nil {
		t.Fatal(err)
	}

	d, err = client.RcloneListDir(ctx, testHost, remotePath(""), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(d))
	}
}
