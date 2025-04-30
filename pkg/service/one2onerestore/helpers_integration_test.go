// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

type testHelper struct {
	client                   *scyllaclient.Client
	clusterID, taskID, runID uuid.UUID
	props                    []byte
	backupSvc                *backup.Service
	restoreSvc               Servicer
}

func newTestHelper(t *testing.T, hosts []string) *testHelper {
	clientCfg := scyllaclient.TestConfig(hosts, AgentAuthToken())
	sc, err := scyllaclient.NewClient(clientCfg, log.NopLogger)
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
	session := CreateScyllaManagerDBSession(t)

	clusterID := uuid.NewTime()
	taskID := uuid.NewTime()
	runID := uuid.NewTime()

	backupSvc := newBackupSvc(t, session, sc, clusterID)
	restoreSvc := newRestoreSvc(t, session, sc, clusterID, "", "")

	return &testHelper{
		client:     sc,
		clusterID:  clusterID,
		taskID:     taskID,
		runID:      runID,
		backupSvc:  backupSvc,
		restoreSvc: restoreSvc,
	}
}

func (h *testHelper) runBackup(t *testing.T, props map[string]any) string {
	t.Helper()
	Printf("Run backup with properties: %v", props)
	ctx := context.Background()
	taskID := uuid.NewTime()
	runID := uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	target, err := h.backupSvc.GetTarget(ctx, h.clusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "generate target"))
	}

	err = h.backupSvc.Backup(ctx, h.clusterID, taskID, runID, target)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	pr, err := h.backupSvc.GetProgress(ctx, h.clusterID, taskID, runID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}

	return pr.SnapshotTag
}

func (h *testHelper) runRestore(t *testing.T, props map[string]any) {
	t.Helper()
	Printf("Run 1-1-restore with properties: %v", props)
	ctx := context.Background()
	h.taskID = uuid.NewTime()
	h.runID = uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}
	h.props = rawProps

	err = h.restoreSvc.One2OneRestore(ctx, h.clusterID, h.taskID, h.runID, h.props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run 1-1-restore"))
	}
}

func testLocation(bucket, dc string) backupspec.Location {
	return backupspec.Location{
		DC:       dc,
		Provider: backupspec.S3,
		Path:     "restoretest-" + bucket,
	}
}

func newBackupSvc(t *testing.T, mgrSession gocqlx.Session, client *scyllaclient.Client, clusterID uuid.UUID) *backup.Service {
	svc, err := backup.NewService(
		mgrSession,
		defaultBackupTestConfig(),
		metrics.NewBackupMetrics(),
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateSession(t, client), nil
		},
		NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func newRestoreSvc(t *testing.T, mgrSession gocqlx.Session, client *scyllaclient.Client, clusterID uuid.UUID, user, pass string) Servicer {
	configCacheSvc := NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts)

	svc, err := NewService(
		mgrSession,
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		configCacheSvc,
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("1-1-restore"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return svc
}

func defaultBackupTestConfig() backup.Config {
	return backup.Config{
		DiskSpaceFreeMinPercent:   5,
		LongPollingTimeoutSeconds: 1,
		AgeMax:                    24 * time.Hour,
	}
}

// getNodeMappings creates []nodeMapping from the cluster that can be reached by client.
// Nodes are mapped to themselves.
func getNodeMappings(t *testing.T, client *scyllaclient.Client) []nodeMapping {
	t.Helper()
	ctx := context.Background()

	var result []nodeMapping

	nodesStatus, err := client.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	for _, n := range nodesStatus {
		rack, err := client.HostRack(ctx, n.Addr)
		if err != nil {
			t.Fatalf("get host rack: %v", err)
		}
		result = append(result, nodeMapping{
			Source: node{DC: n.Datacenter, Rack: rack, HostID: n.HostID},
			Target: node{DC: n.Datacenter, Rack: rack, HostID: n.HostID},
		})
	}

	return result
}
