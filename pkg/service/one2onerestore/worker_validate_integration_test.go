// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
)

func TestWorkerValidateClustersIntegration(t *testing.T) {
	loc := backupspec.Location{
		Provider: backupspec.S3,
		Path:     "my-1-1-restore-test",
	}
	testutils.S3InitBucket(t, loc.Path)

	w, hrt := newTestWorker(t, testconfig.ManagedClusterHosts())
	h := newTestHelper(t, testconfig.ManagedClusterHosts())
	snapshotTag := h.runBackup(t, map[string]any{
		"location": []backupspec.Location{loc},
	})

	manifests, hosts, err := w.getAllSnapshotManifestsAndTargetHosts(context.Background(), Target{
		SourceClusterID: h.clusterID,
		SnapshotTag:     snapshotTag,
		Location:        []backupspec.Location{loc},
	})
	if err != nil {
		t.Fatalf("Unexpected err, getAllSnapshotManifestsAndTargetHosts: %v", err)
	}

	nodeMappings := getNodeMappings(t, w.client)

	testCases := []struct {
		name                 string
		hostsProvider        func() []Host
		manifestsProvider    func() []*backupspec.ManifestInfo
		nodeMappingsProvider func() []nodeMapping
		setInterceptor       func()
		expectedErr          bool
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
			expectedErr: false,
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
			expectedErr: true,
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
			expectedErr: true,
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
			expectedErr: true,
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
			expectedErr: true,
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
			expectedErr: true,
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
			setInterceptor: func() {
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
			expectedErr: true,
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
			setInterceptor: func() {
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
			expectedErr: true,
		},
		{
			name: "Node tokens mismatch",
			manifestsProvider: func() []*backupspec.ManifestInfo {
				return manifests
			},
			hostsProvider: func() []Host {
				return hosts
			},
			nodeMappingsProvider: func() []nodeMapping {
				return nodeMappings
			},
			setInterceptor: func() {
				randomNode := hosts[rand.IntN(len(hosts))].Addr
				hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if req.URL.Path != "/storage_service/tokens/"+randomNode {
						return nil, nil
					}
					resp := httpx.MakeResponse(req, http.StatusOK)
					resp.Body = io.NopCloser(strings.NewReader(`["-5","-4","-3","-2","-1","0","1","2","3","4","5"]`))
					return resp, nil
				}))
			},
			expectedErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setInterceptor != nil {
				tc.setInterceptor()
			}

			err := w.validateClusters(context.Background(), tc.manifestsProvider(), tc.hostsProvider(), tc.nodeMappingsProvider())
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			hrt.SetInterceptor(nil)
		})
	}
}

func newTestWorker(t *testing.T, hosts []string) (*worker, *testutils.HackableRoundTripper) {
	t.Helper()
	hrt := testutils.NewHackableRoundTripper(scyllaclient.DefaultTransport())
	cfg := scyllaclient.TestConfig(hosts, testutils.AgentAuthToken())
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
