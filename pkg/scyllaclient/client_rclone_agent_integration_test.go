// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package scyllaclient_test

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

var longPollingTimeoutSeconds = 1

func TestRcloneS3ListDirAgentIntegration(t *testing.T) {
	testHost := ManagedClusterHost()

	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	S3InitBucket(t, testBucket)

	ctx := context.Background()

	d, err := client.RcloneListDir(ctx, testHost, remotePath(""), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(d))
	}
}

func TestRcloneSkippingFilesAgentIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	testHost := ManagedClusterHost()

	S3InitBucket(t, testBucket)

	ctx := context.Background()

	// Create test directory with files on the test host.
	cmd := injectDataDir("rm -rf %s/tmp/copy && mkdir -p %s/tmp/copy && echo 'bar' > %s/tmp/copy/foo && echo 'foo' > %s/tmp/copy/bar")
	_, _, err = ExecOnHost(testHost, cmd)
	if err != nil {
		t.Fatal(err)
	}
	id, err := client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp/copy", "")
	if err != nil {
		t.Fatal(err)
	}

	var job *scyllaclient.RcloneJobInfo
	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Transferred) == 2
	}, 50*time.Millisecond, time.Second)

	for _, r := range job.Transferred {
		if r.Checked == true {
			t.Errorf("Expected transferred files to not be checked")
		}
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}

	id, err = client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp/copy", "")
	if err != nil {
		t.Fatal(err)
	}

	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Transferred) == 2
	}, 50*time.Millisecond, time.Second)

	for _, r := range job.Transferred {
		if r.Checked == false {
			t.Errorf("Expected transferred files to be checked")
		}
		if r.Error != "" {
			t.Errorf("Expected no error got: %s, %v", r.Error, r)
		}
	}
}

func TestRcloneStoppingTransferIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	testHost := ManagedClusterHost()

	S3InitBucket(t, testBucket)

	ctx := context.Background()

	if err := client.RcloneSetBandwidthLimit(ctx, testHost, 1); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.RcloneSetBandwidthLimit(ctx, testHost, 0); err != nil {
			t.Fatal(err)
		}
	}()

	// Create big enough file on the test host to keep running for long enough
	// 100MiB = 1024 * 102400.
	cmd := injectDataDir("rm -rf %s/tmp/copy && mkdir -p %s/tmp/ && dd if=/dev/zero of=%s/tmp/copy count=1024 bs=102400")
	_, _, err = ExecOnHost(testHost, cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cmd := injectDataDir("rm -rf %s/tmp")
		_, _, err := ExecOnHost(testHost, cmd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp", "")
	if err != nil {
		t.Fatal(err)
	}

	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Stats.Transferring) > 0
	}, time.Second, 3*time.Second)

	err = client.RcloneJobStop(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
	if err != nil {
		t.Fatal(err)
	}

	if job.Transferred[0].Error == "" {
		t.Fatal("Expected error but got empty")
	}
}

func TestRcloneSuffixOptionIntegration(t *testing.T) {
	var (
		// "backup/meta" prefix in bucket path allows for using rclone cat
		srcPath = "data:tmp/backup/meta"
		srcFile = path.Join(srcPath, "file")
		dstPath = remotePath("backup/meta")
	)

	const (
		firstContents  = "First file!"
		secondContents = "Second file!"
		thirdContents  = "Third file!"
		firstSuffix    = ".suffix_1"
		secondSuffix   = ".suffix_2"
	)

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	testHost := ManagedClusterHost()
	S3InitBucket(t, testBucket)
	ctx := context.Background()
	// This test also uses data from local data dir, so it needs to perform cleanup at the end
	defer func() {
		cmd := injectDataDir("rm -rf %s/tmp")
		_, _, err := ExecOnHost(testHost, cmd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	Print("Put first file in src dir")

	b := bytes.NewBuffer([]byte(firstContents))
	if err = client.RclonePut(ctx, testHost, srcFile, b); err != nil {
		t.Fatal(err)
	}
	// Validate contents of src and dst dirs after each step
	srcM := map[string]string{
		"file": firstContents,
	}
	dstM := map[string]string{}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}

	Print("Copy src into dst")

	id, err := client.RcloneCopyDir(ctx, testHost, dstPath, srcPath, "")
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess
	}, time.Second, 3*time.Second)

	srcM = map[string]string{
		"file": firstContents,
	}
	dstM = map[string]string{
		"file": firstContents,
	}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}

	Print("Modify src file")

	b = bytes.NewBuffer([]byte(secondContents))
	if err = client.RclonePut(ctx, testHost, srcFile, b); err != nil {
		t.Fatal(err)
	}

	srcM = map[string]string{
		"file": secondContents,
	}
	dstM = map[string]string{
		"file": firstContents,
	}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}

	Print("Copy src into dst after file modification")

	id, err = client.RcloneCopyDir(ctx, testHost, dstPath, srcPath, firstSuffix)
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess
	}, time.Second, 3*time.Second)

	srcM = map[string]string{
		"file": secondContents,
	}
	dstM = map[string]string{
		"file" + firstSuffix: firstContents,
		"file":               secondContents,
	}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}

	Print("Modify src file again")

	b = bytes.NewBuffer([]byte(thirdContents))
	if err = client.RclonePut(ctx, testHost, srcFile, b); err != nil {
		t.Fatal(err)
	}

	srcM = map[string]string{
		"file": thirdContents,
	}
	dstM = map[string]string{
		"file" + firstSuffix: firstContents,
		"file":               secondContents,
	}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}

	Print("Copy src into dst after another file modification")

	id, err = client.RcloneCopyDir(ctx, testHost, dstPath, srcPath, secondSuffix)
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess
	}, time.Second, 3*time.Second)

	srcM = map[string]string{
		"file": thirdContents,
	}
	dstM = map[string]string{
		"file" + firstSuffix:  firstContents,
		"file" + secondSuffix: secondContents,
		"file":                thirdContents,
	}
	if err := validateDirContents(ctx, client, testHost, srcPath, srcM); err != nil {
		t.Fatal(err)
	}
	if err := validateDirContents(ctx, client, testHost, dstPath, dstM); err != nil {
		t.Fatal(err)
	}
}

func validateDirContents(ctx context.Context, client *scyllaclient.Client, host, remotePath string, files map[string]string) error {
	// Check if all specified files with given contents are present
	for f, expected := range files {
		gotBytes, err := client.RcloneCat(ctx, host, path.Join(remotePath, f))
		if err != nil {
			return err
		}
		got := string(gotBytes)
		if got != expected {
			return errors.New(fmt.Sprintf("checking file %s, expected: %s, got: %s", f, expected, got))
		}
	}
	// Check if these are the only files present
	var iterErr error
	err := client.RcloneListDirIter(ctx, host, remotePath, nil, func(item *scyllaclient.RcloneListDirItem) {
		_, ok := files[item.Name]
		if !ok {
			iterErr = errors.New(fmt.Sprintf("didn't expect file: %s to be present", item.Name))
		}
	})
	if err != nil {
		return err
	}
	return iterErr
}

const scyllaDataDir = "/var/lib/scylla/data"

func injectDataDir(cmd string) string {
	return strings.ReplaceAll(cmd, "%s", scyllaDataDir)
}
