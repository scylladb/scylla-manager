// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestWorkerValidateClustersIntegration(t *testing.T) {
	loc := backupspec.Location{
		Provider: backupspec.S3,
		Path:     "my-1-1-restore-test",
	}
	testutils.S3InitBucket(t, loc.Path)

	w, hrt := testNewWorker(t)

	clusterID := uuid.MustRandom()
	backupSvc := newBackupSvc(t, db.CreateScyllaManagerDBSession(t), w.client, clusterID)
	snapshotTag := runBackup(t, backupSvc, clusterID, map[string]any{
		"location": []backupspec.Location{loc},
	})

	manifests, hosts, err := w.getManifestsAndHosts(context.Background(), Target{
		SourceClusterID: clusterID,
		SnapshotTag:     snapshotTag,
		Location:        []backupspec.Location{loc},
	})
	if err != nil {
		t.Fatalf("Unexpected err, getManifestsAndHosts: %v", err)
	}

	nodeMappings := getNodeMappings(t, w.client)

	testCases := []struct {
		name                 string
		hostsProvider        func() []Host
		manifestsProvider    func() []*backupspec.ManifestInfo
		nodeMappingsProvider func() []nodeMapping
		setIntereptor        func()
		expecterErr          bool
	}{

		{
			name: "Source cluster == Target cluster",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			setIntereptor: func() {
				hrt.SetInterceptor(nil)
			},
			expecterErr: false,
		},
		{
			name: "Less nodes in target clusters",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts[1:]
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			setIntereptor: func() {
				hrt.SetInterceptor(nil)
			},
			expecterErr: true,
		},
		{
			name: "Less nodes in source clusters",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests[1:]
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			expecterErr: true,
		},
		{
			name: "Less nodes in nodes mappings",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings[1:]
			},
			setIntereptor: func() {
				hrt.SetInterceptor(nil)
			},
			expecterErr: true,
		},
		{
			name: "Wrong source nodes mapping",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				modified := make([]nodeMapping, len(nodeMappings))
				copy(modified, nodeMappings)
				modified[0].Source.DC = "not found"
				return modified
			},
			setIntereptor: func() {
				hrt.SetInterceptor(nil)
			},
			expecterErr: true,
		},
		{
			name: "Wrong target nodes mapping",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				modified := make([]nodeMapping, len(nodeMappings))
				copy(modified, nodeMappings)
				modified[0].Target.DC = "not found"
				return modified
			},
			setIntereptor: func() {
				hrt.SetInterceptor(nil)
			},
			expecterErr: true,
		},
		{
			name: "Node doesn't have access to manifest location",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			setIntereptor: func() {
				randomNode := hosts[rand.IntN(len(hosts))].Addr
				hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if req.URL.Hostname() != randomNode {
						return nil, nil
					}
					if req.URL.Path != "/agent/rclone/operations/cat" {
						return nil, nil
					}
					return httpx.MakeResponse(req, http.StatusBadRequest), nil
				}))
			},
			expecterErr: true,
		},
		{
			name: "Node is not alive",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			setIntereptor: func() {
				randomNode := hosts[rand.IntN(len(hosts))].Addr
				hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if req.URL.Hostname() != randomNode {
						return nil, nil
					}
					if req.URL.Path != "/storage_service/scylla_release_version" {
						return nil, nil
					}
					return httpx.MakeResponse(req, http.StatusBadRequest), nil
				}))
			},
			expecterErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setIntereptor != nil {
				tc.setIntereptor()
			}

			err := w.validateClusters(context.Background(), tc.manifestsProvider(), tc.hostsProvider(), tc.nodeMappingsProvider())
			if tc.expecterErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expecterErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
		})
	}
}

func testNewWorker(t *testing.T) (*worker, *testutils.HackableRoundTripper) {
	t.Helper()
	hrt := testutils.NewHackableRoundTripper(scyllaclient.DefaultTransport())
	cfg := scyllaclient.TestConfig(testconfig.ManagedSecondClusterHosts(), testutils.AgentAuthToken())
	cfg.Transport = hrt
	sc, err := scyllaclient.NewClient(cfg, log.NopLogger)
	if err != nil {
		t.Fatalf("new scylla client: %v", err)
	}

	managerSession := db.CreateManagedClusterSession(t, false, sc, "", "")
	clusterSession := db.CreateSession(t, sc)

	w := &worker{
		managerSession: managerSession,
		client:         sc,
		clusterSession: clusterSession,
		logger:         log.NopLogger,
	}
	return w, hrt
}

func newBackupSvc(t *testing.T, mgrSession gocqlx.Session, client *scyllaclient.Client, clusterID uuid.UUID) *backup.Service {
	t.Helper()
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
			return db.CreateSession(t, client), nil
		},
		testhelper.NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		log.NopLogger,
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

func runBackup(t *testing.T, backupSvc *backup.Service, clusterID uuid.UUID, props map[string]any) string {
	t.Helper()
	testutils.Printf("Run backup with properties: %v", props)
	ctx := context.Background()
	taskID := uuid.NewTime()
	runID := uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	target, err := backupSvc.GetTarget(ctx, clusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "generate target"))
	}

	err = backupSvc.Backup(ctx, clusterID, taskID, runID, target)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	pr, err := backupSvc.GetProgress(ctx, clusterID, taskID, runID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	return pr.SnapshotTag
}

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
