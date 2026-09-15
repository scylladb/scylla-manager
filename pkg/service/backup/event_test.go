// Copyright (C) 2026 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// jsonLogger builds a logger with the same encoder Scylla Manager uses in
// production, so the test asserts on the exact bytes that reach journald.
func jsonLogger(buf *bytes.Buffer) log.Logger {
	enc := zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(enc), zapcore.AddSync(buf), zapcore.DebugLevel)
	return log.NewLogger(zap.New(core))
}

func TestLogBackupCompleted(t *testing.T) {
	var (
		clusterID = uuid.MustParse("6f1d5c3e-0000-4000-8000-000000000001")
		taskID    = uuid.MustParse("7a2e6d4f-0000-4000-8000-000000000002")
		runID     = uuid.MustParse("8b3f7e50-0000-4000-8000-000000000003")
	)

	var buf bytes.Buffer
	w := &worker{
		workerTools: workerTools{
			ClusterID:   clusterID,
			ClusterName: "test-cluster",
			TaskID:      taskID,
			RunID:       runID,
			SnapshotTag: "sm_20260915120000UTC",
			Logger:      jsonLogger(&buf),
		},
	}

	loc := backupspec.Location{Provider: backupspec.S3, Path: "backup-bucket-1-4"}
	hosts := []hostInfo{
		{DC: "dc1", IP: "10.0.0.1", ID: "node-a", Location: loc},
		{DC: "dc1", IP: "10.0.0.2", ID: "node-b", Location: loc},
	}

	w.logBackupCompleted(context.Background(), hosts)

	var got struct {
		Message     string        `json:"M"`
		Event       string        `json:"sm_event"`
		ClusterID   string        `json:"cluster_id"`
		ClusterName string        `json:"cluster_name"`
		TaskID      string        `json:"task_id"`
		RunID       string        `json:"run_id"`
		SnapshotTag string        `json:"snapshot_tag"`
		Manifests   []manifestRef `json:"manifests"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode log line: %v\nline: %s", err, buf.String())
	}

	if got.Event != eventBackupCompleted {
		t.Errorf("sm_event = %q, expected %q", got.Event, eventBackupCompleted)
	}
	if got.ClusterID != clusterID.String() {
		t.Errorf("cluster_id = %q, expected %q", got.ClusterID, clusterID)
	}
	if got.TaskID != taskID.String() {
		t.Errorf("task_id = %q, expected %q", got.TaskID, taskID)
	}
	if got.RunID != runID.String() {
		t.Errorf("run_id = %q, expected %q", got.RunID, runID)
	}
	if got.SnapshotTag != "sm_20260915120000UTC" {
		t.Errorf("snapshot_tag = %q", got.SnapshotTag)
	}
	if len(got.Manifests) != len(hosts) {
		t.Fatalf("got %d manifests, expected %d", len(got.Manifests), len(hosts))
	}

	// Every ref must round-trip through the parser the consumer uses, and must
	// agree with the metadata carried alongside it.
	for i, ref := range got.Manifests {
		if ref.Bucket != loc.Path || ref.Provider != string(loc.Provider) {
			t.Errorf("manifest[%d] location = %s:%s, expected %s:%s", i, ref.Provider, ref.Bucket, loc.Provider, loc.Path)
		}
		var mi backupspec.ManifestInfo
		if err := mi.ParsePath(ref.Path); err != nil {
			t.Fatalf("manifest[%d] path %q is not parseable: %v", i, ref.Path, err)
		}
		if mi.Temporary {
			t.Errorf("manifest[%d] path %q points at a temporary manifest", i, ref.Path)
		}
		if mi.ClusterID != clusterID || mi.TaskID != taskID {
			t.Errorf("manifest[%d] parsed cluster/task = %s/%s", i, mi.ClusterID, mi.TaskID)
		}
		if mi.SnapshotTag != "sm_20260915120000UTC" {
			t.Errorf("manifest[%d] parsed snapshot tag = %q", i, mi.SnapshotTag)
		}
		if mi.DC != ref.DC || mi.NodeID != ref.NodeID {
			t.Errorf("manifest[%d] parsed dc/node = %s/%s, ref says %s/%s", i, mi.DC, mi.NodeID, ref.DC, ref.NodeID)
		}
	}

	t.Logf("event line: %s", buf.String())
}
