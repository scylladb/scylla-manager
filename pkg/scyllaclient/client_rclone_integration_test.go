// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package scyllaclient_test

import (
	"context"
	"path"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

var listRecursively = &scyllaclient.RcloneListDirOpts{Recurse: true}

const (
	testRemote = "s3"
	testBucket = "backuptest-rclone"
)

func remotePath(p string) string {
	return path.Join(testRemote+":"+testBucket, p)
}

func TestRcloneLocalToS3CopyDirIntegration(t *testing.T) {
	S3InitBucket(t, testBucket)

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	copyDir := func(dir string) (*scyllaclient.RcloneJobInfo, error) {
		id, err := client.RcloneCopyDir(ctx, scyllaclienttest.TestHost, scyllaclient.TransfersFromConfig, remotePath("/copy"), "rclonetest:"+dir, "")
		if err != nil {
			t.Fatal(err)
		}
		defer client.RcloneDeleteJobStats(ctx, scyllaclienttest.TestHost, id)
		return client.RcloneJobInfo(ctx, scyllaclienttest.TestHost, id, longPollingTimeoutSeconds)
	}

	Print("When: Copy dir")
	job, err := copyDir("copy")
	if err != nil {
		t.Fatal(err)
	}
	Print("Then: Job ends successfully")
	if !job.Job.Finished || !job.Job.Success {
		t.Log(job.Job)
		t.Errorf("Expected copy dir job to finish successfully")
	}
	Print("And: 2 files are transferred")
	if len(job.Transferred) != 2 {
		t.Errorf("Expected 2 transferred files got %d", len(job.Transferred))
	}
	for _, r := range job.Transferred {
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}
	d, err := client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 3 {
		t.Errorf("Expected bucket have 3 items, got: len(files)=%d", len(d))
	}

	Print("When: Try to overwrite files")
	job, err = copyDir("copy-modified")
	if err != nil {
		t.Fatal(err)
	}
	if rclone.GetConfig().Immutable {
		t.Fatal("Immutable shall be disabled")
	}
	Print("Then: Job ends successfully")
	if !job.Job.Finished || !job.Job.Success {
		t.Log(job.Job)
		t.Errorf("Expected copy-modified dir job to finish successfully")
	}

	Print("When: Delete dir")
	if err = client.RcloneDeleteDir(ctx, scyllaclienttest.TestHost, remotePath("/copy")); err != nil {
		t.Fatal(err)
	}
	Print("Then: Directory is removed")
	d, err = client.RcloneListDir(ctx, scyllaclienttest.TestHost, remotePath("/copy"), listRecursively)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: %v", d)
	}
}

func TestRcloneS3ToLocalCopyDirIntegration(t *testing.T) {
	S3InitBucket(t, testBucket)

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()
	ctx := context.Background()

	id, err := client.RcloneCopyDir(ctx, scyllaclienttest.TestHost, scyllaclient.TransfersFromConfig, "rclonetest:foo", remotePath("/copy"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.RcloneDeleteJobStats(ctx, scyllaclienttest.TestHost, id)
	info, err := client.RcloneJobInfo(ctx, scyllaclienttest.TestHost, id, longPollingTimeoutSeconds)
	if err != nil {
		t.Fatal(err)
	}
	if info.Job.Error != "permission denied" {
		t.Fatalf("RcloneJobInfo() = %v, expected permission denied", *info.Job)
	}
}
