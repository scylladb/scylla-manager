// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	. "github.com/scylladb/mermaid/pkg/testutils"
)

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
	id, err := client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp/copy")
	if err != nil {
		t.Fatal(err)
	}

	var job *scyllaclient.RcloneJobInfo
	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, testHost, id)
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

	id, err = client.RcloneCopyDir(ctx, testHost, remotePath(""), "data:tmp/copy")
	if err != nil {
		t.Fatal(err)
	}

	WaitCond(t, func() bool {
		job, err = client.RcloneJobInfo(ctx, testHost, id)
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
	config.LongPollingSeconds = 1
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

	// Create big enough file on the test host to keep running for long enough.
	// 1024*102400
	cmd := injectDataDir("rm -rf %s/tmp/copy && mkdir -p %s/tmp/ && dd if=/dev/zero of=%s/tmp/copy count=1024 bs=102400")
	_, _, err = ExecOnHost(testHost, cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cmd := fmt.Sprintf("rm -rf %s/tmp/copy", scyllaDataDir)
		_, _, err := ExecOnHost(testHost, cmd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := client.RcloneCopyFile(ctx, testHost, remotePath("/copy"), "data:tmp/copy")
	if err != nil {
		t.Fatal(err)
	}

	WaitCond(t, func() bool {
		job, err := client.RcloneJobInfo(ctx, testHost, id)
		if err != nil {
			t.Fatal(err)
		}
		return len(job.Stats.Transferring) > 0
	}, time.Second, 3*time.Second)

	err = client.RcloneJobStop(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	job, err := client.RcloneJobInfo(ctx, testHost, id)
	if err != nil {
		t.Fatal(err)
	}

	if job.Transferred[0].Error == "" {
		t.Fatal("Expected error but got empty")
	}
}

const scyllaDataDir = "/var/lib/scylla/data"

func injectDataDir(cmd string) string {
	return strings.ReplaceAll(cmd, "%s", scyllaDataDir)
}
