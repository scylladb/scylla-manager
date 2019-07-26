// Copyright (C) 2017 ScyllaDB

// +build all integration

package backup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/service/backup"
)

func TestRunnerStoppingBackupIntegration(t *testing.T) {
	const bucket = "runnertest"

	S3InitBucket(t, bucket)

	config := backup.DefaultConfig()
	config.TestS3Endpoint = S3TestEndpoint()

	var (
		session     = CreateSession(t)
		h           = newBackupTestHelper(t, session, config)
		ctx, cancel = context.WithCancel(context.Background())
	)

	properties := json.RawMessage(fmt.Sprintf(`{
		"keyspace": [
			"system_auth"
		],
		"dc": ["dc1"],
		"location": ["%s:%s"],
		"retention": 1,
		"rate_limit": ["dc1:1"]
	}`, backup.S3, bucket))

	done := make(chan struct{})
	go func() {
		Print("When: runner is running")
		err := h.runner.Run(ctx, h.clusterID, h.taskID, h.runID, properties)
		if err == nil {
			t.Error("Expected error on run but got nil")
		}
		close(done)
	}()

	for {
		time.Sleep(5 * time.Millisecond)
		s, err := h.client.RcloneStats(context.Background(), ManagedClusterHosts[0], "")
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Transferring) > 0 {
			Print("And: upload is underway")
			break
		}
	}

	Print("And: context is canceled")
	cancel()
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond) // Wait for cancel to propagate to stats

	s, err := h.client.RcloneStats(context.Background(), ManagedClusterHosts[0], "")
	if err != nil {
		t.Fatal(err)
	}

	Print("And: nothing is transferring")
	if len(s.Transferring) > 0 {
		t.Error("Expected no completed transfers", fmt.Sprintf("%+v", s))
	}

	Print("And: runner completed execution")
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Runner didn't complete in under 5sec")
	case <-done:
		// continue
	}
}
