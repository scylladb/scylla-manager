// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"path"
	"testing"
	"time"

	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/models"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
)

var listRecursively = &scyllaclient.RcloneListDirOpts{Recurse: true}

const (
	testRemote = "s3"
	testBucket = "rclonetest"
)

func remotePath(p string) string {
	return path.Join(testRemote+":"+testBucket, p)
}

func TestRcloneLocalToS3CopyDirIntegration(t *testing.T) {
	S3InitBucket(t, testBucket)

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	ctx := context.Background()

	id, err := client.RcloneCopyDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), "rclonetest:testdata/rclone/copy")
	if err != nil {
		t.Fatal(err)
	}

	var transferred []*models.Transfer
	WaitCond(t, func() bool {
		var err error
		transferred, err = client.RcloneTransferred(ctx, scyllaclienttest.TestHost, scyllaclient.RcloneDefaultGroup(id))
		if err != nil {
			t.Fatal(err)
		}
		return len(transferred) == 2
	}, 50*time.Millisecond, time.Second)

	for _, r := range transferred {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	status, err := client.RcloneJobStatus(ctx, scyllaclienttest.TestHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy dir job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 3 {
		t.Errorf("Expected bucket have 3 items, got: len(files)=%d", len(d))
	}

	if err = client.RcloneDeleteDir(ctx, scyllaclienttest.TestHost, remotePath("/copy")); err != nil {
		t.Fatal(err)
	}

	d, err = client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: %v", d)
	}
}

func TestRcloneLocalToS3CopyFileIntegration(t *testing.T) {
	S3InitBucket(t, testBucket)

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	ctx := context.Background()

	id, err := client.RcloneCopyFile(ctx, scyllaclienttest.TestHost, remotePath("/file2.txt"), "rclonetest:testdata/rclone/copy/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	var transferred []*models.Transfer
	WaitCond(t, func() bool {
		var err error
		transferred, err = client.RcloneTransferred(ctx, scyllaclienttest.TestHost, scyllaclient.RcloneDefaultGroup(id))
		if err != nil {
			t.Fatal(err)
		}
		return len(transferred) == 1
	}, 50*time.Millisecond, time.Second)

	for _, r := range transferred {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	status, err := client.RcloneJobStatus(ctx, scyllaclienttest.TestHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if !status.Finished || !status.Success {
		t.Log(status)
		t.Errorf("Expected copy file job to finish successfully")
	}

	d, err := client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath(""), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 1 {
		t.Errorf("Expected bucket have 1 item, got: len(files)=%d", len(d))
	}

	if err := client.RcloneDeleteFile(ctx, scyllaclienttest.TestHost, remotePath("/file2.txt")); err != nil {
		t.Fatal(err)
	}

	d, err = client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath(""), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(d))
	}
}
