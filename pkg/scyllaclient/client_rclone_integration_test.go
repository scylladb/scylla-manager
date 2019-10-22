// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/mermaid/pkg/testutils"
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

	client, closeServer :=scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	id, err := client.RcloneCopyDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), "rclonetest:testdata/rclone/copy")
	if err != nil {
		t.Fatal(err)
	}
	defer client.RcloneDeleteJobStats(ctx, scyllaclienttest.TestHost, id)

	var job *scyllaclient.RcloneJobInfo
	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, scyllaclienttest.TestHost, id)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Transferred) == 2
	}, 50*time.Millisecond, time.Second)

	for _, r := range job.Transferred {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	if err != nil {
		t.Fatal(err)
	}

	if !job.Job.Finished || !job.Job.Success {
		t.Log(job.Job)
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

	client, closeServer :=scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	id, err := client.RcloneCopyFile(ctx, scyllaclienttest.TestHost, remotePath("/file2.txt"), "rclonetest:testdata/rclone/copy/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer client.RcloneDeleteJobStats(ctx, scyllaclienttest.TestHost, id)

	var job *scyllaclient.RcloneJobInfo
	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, scyllaclienttest.TestHost, id)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Transferred) == 1
	}, 50*time.Millisecond, time.Second)

	for _, r := range job.Transferred {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	if !job.Job.Finished || !job.Job.Success {
		t.Log(job.Job)
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
