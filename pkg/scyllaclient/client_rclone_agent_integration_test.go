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
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"go.uber.org/zap/zapcore"
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

func TestRcloneDeletePathsInBatchesAgentIntegration(t *testing.T) {
	var (
		ctx      = context.Background()
		testHost = ManagedClusterHost()
		dirName  = "tmp/copy"
	)

	S3InitBucket(t, testBucket)
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopmentWithLevel(zapcore.ErrorLevel))
	if err != nil {
		t.Fatal(err)
	}

	const bigSize = 3456
	lotsOfFiles := make([]string, 0, bigSize)
	for i := 0; i < bigSize; i++ {
		lotsOfFiles = append(lotsOfFiles, fmt.Sprint(i))
	}

	testCases := []struct {
		name      string
		before    []string
		delete    []string
		after     []string
		batchSize int
	}{
		{
			name:      "delete all",
			before:    []string{"a", "b", "c"},
			delete:    []string{"a", "b", "c"},
			after:     nil,
			batchSize: 1,
		},
		{
			name:      "delete nothing",
			before:    []string{"a", "b", "c"},
			delete:    []string{},
			after:     []string{"a", "b", "c"},
			batchSize: 1,
		},
		{
			name:      "delete only non-existing files",
			before:    []string{"a", "b", "c", "d"},
			delete:    []string{"e", "f", "g"},
			after:     []string{"a", "b", "c", "d"},
			batchSize: 5,
		},
		{
			name:      "delete subset with non-existing file",
			before:    []string{"a", "b", "c", "d"},
			delete:    []string{"a", "c", "e"},
			after:     []string{"b", "d"},
			batchSize: 2,
		},
		{
			name:      "delete thousands of files",
			before:    append([]string{"a"}, lotsOfFiles...),
			delete:    append([]string{}, lotsOfFiles...),
			after:     []string{"a"},
			batchSize: 1000,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			cmd := []string{injectDataDir("rm -rf %s/" + dirName), injectDataDir("mkdir -p %s/" + dirName)}
			for _, name := range tc.before {
				cmd = append(cmd, injectDataDir("echo 'dummy-file' > %s/"+path.Join(dirName, name)))
			}
			for startPos := 0; startPos < len(cmd); startPos += 100 {
				batch := cmd[startPos : startPos+min(100, len(cmd)-startPos)]
				stdOut, stdErr, err := ExecOnHost(testHost, strings.Join(batch, " && "))
				if err != nil {
					t.Fatalf("Create files on Scylla node, err = {%s}, stdOut={%s}, stdErr={%s}", err, stdOut, stdErr)
				}
			}
			id, err := client.RcloneCopyDir(ctx, testHost, remotePath(dirName), "data:"+dirName, "")
			if err != nil {
				t.Fatal(errors.Wrap(err, "copy created files to backup location"))
			}

			WaitCond(t, func() bool {
				pr, err := client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
				if err != nil {
					t.Fatal(errors.Wrap(err, "wait for copy to finish"))
				}
				switch scyllaclient.RcloneJobStatus(pr.Status) {
				case scyllaclient.JobSuccess:
					return true
				case scyllaclient.JobError:
					t.Errorf("wait for copy to finish: %s", pr.Error)
				}
				return false
			}, 50*time.Millisecond, 5*time.Minute)

			for _, remote := range []string{"data:" + dirName, remotePath(dirName)} {
				if len(tc.before) < 1000 {
					Printf("Given: files %v in remote %s", tc.before, remote)
				}
				if err := validateDir(ctx, client, testHost, remote, tc.before); err != nil {
					t.Error(errors.Wrapf(err, "%s: validate dir conetnts before paths deletion", remote))
				}

				// Delete paths twice in order to validate the expected amount of deleted files
				for i := 0; i < 2; i++ {
					if len(tc.delete) < 1000 {
						Printf("When: delete paths %v", tc.delete)
					}
					_, err := client.RcloneDeletePathsInBatches(ctx, testHost, remote, tc.delete, tc.batchSize)
					if err != nil {
						t.Error(errors.Wrapf(err, "%s: delete paths", remote))
					}

					//Printf("Then: validate the amount of deleted files")
					//if cnt != expected {
					//	t.Error(errors.Errorf("%s: expected: %d, got: %d", remote, expected, cnt))
					//}

					if len(tc.after) < 1000 {
						Printf("And: validate that the only files left are %v", tc.after)
					}
					if err := validateDir(ctx, client, testHost, remote, tc.after); err != nil {
						t.Error(errors.Wrapf(err, "%s: validate dir conetnt after paths deletion", remote))
					}
				}

				if err := client.RcloneDeleteDir(ctx, testHost, remote); err != nil {
					t.Error(errors.Wrapf(err, "%s: clean up created dirs", remote))
				}
			}
		})
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

	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		if len(job.Transferred) > 0 {
			if job.Transferred[0].Error == "" {
				t.Fatal("Expected error but got empty")
			}
			return true
		}
		return false
	}, time.Second, 3*time.Second)
}

func TestRcloneJobProgressIntegration(t *testing.T) {
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

	firstBatchSize := 1024 * 1024
	cmd := injectDataDir("rm -rf %s/tmp/copy && mkdir -p %s/tmp/ && dd if=/dev/zero of=%s/tmp/copy1 count=1024 bs=1024")
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

	Print("When: first batch upload")
	id, err := client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp", "")
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return job.Job.Finished
	}, time.Second, 10*time.Second)

	jp, err := client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
	switch {
	case err != nil:
		t.Fatal(err)
	case jp.Failed != 0:
		t.Fatal("expected no failed bytes")
	case jp.Skipped != 0:
		t.Fatal("expected no skipped bytes on first upload")
	case jp.Uploaded != int64(firstBatchSize):
		t.Fatalf("expected exactly %d bytes, got %d", firstBatchSize, jp.Uploaded)
	}

	secondBatchSize := 2048 * 2048
	cmd = injectDataDir("dd if=/dev/zero of=%s/tmp/copy2 count=2048 bs=2048")
	_, _, err = ExecOnHost(testHost, cmd)
	if err != nil {
		t.Fatal(err)
	}

	Print("When: second batch upload")
	id, err = client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp", "")
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return job.Job.Finished
	}, time.Second, 10*time.Second)

	jp, err = client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
	switch {
	case err != nil:
		t.Fatal(err)
	case jp.Failed != 0:
		t.Fatal("expected no failed bytes")
	case jp.Skipped != int64(firstBatchSize):
		t.Fatalf("expected %d skipped bytes from first upload, got %d", firstBatchSize, jp.Skipped)
	case jp.Uploaded != int64(secondBatchSize):
		t.Fatalf("expected exactly %d bytes, got %d", secondBatchSize, jp.Uploaded)
	}

	Print("When: third batch upload")
	id, err = client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp", "")
	if err != nil {
		t.Fatal(err)
	}
	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id, longPollingTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		return job.Job.Finished
	}, time.Second, 10*time.Second)

	jp, err = client.RcloneJobProgress(ctx, testHost, id, longPollingTimeoutSeconds)
	switch {
	case err != nil:
		t.Fatal(err)
	case jp.Failed != 0:
		t.Fatal("expected no failed bytes")
	case jp.Skipped != int64(firstBatchSize+secondBatchSize):
		t.Fatalf("expected %d skipped bytes from both uploads, got %d", firstBatchSize+secondBatchSize, jp.Skipped)
	case jp.Uploaded != 0:
		t.Fatalf("expected no new uploaded bytes, got %d", jp.Uploaded)
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

func validateDir(ctx context.Context, client *scyllaclient.Client, host, remotePath string, files []string) error {
	encounteredFiles := strset.New()
	err := client.RcloneListDirIter(ctx, host, remotePath, nil, func(item *scyllaclient.RcloneListDirItem) {
		encounteredFiles.Add(item.Path)
	})
	if err != nil {
		return err
	}
	if filesS := strset.New(files...); !filesS.IsEqual(encounteredFiles) {
		return fmt.Errorf("expected dir content: %v, got: %v", files, encounteredFiles.List())
	}
	return nil
}

const scyllaDataDir = "/var/lib/scylla/data"

func injectDataDir(cmd string) string {
	return strings.ReplaceAll(cmd, "%s", scyllaDataDir)
}
