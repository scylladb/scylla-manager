// Copyright (C) 2025 ScyllaDB

//go:build all || integration

package one2onerestore

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestWorkerValidateClustersIntegration(t *testing.T) {
	if tablets := os.Getenv("TABLETS"); tablets == "enabled" {
		t.Skip("1-1-restore is available only for v-nodes")
	}
	loc := backupspec.Location{
		Provider: testconfig.BackupProvider(),
		Path:     "my-1-1-restore-test",
	}
	testutils.InitBucket(t, loc.Path)

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

	t.Run("temporary and shadowed manifests", func(t *testing.T) {
		target := Target{
			SourceClusterID: h.clusterID,
			SnapshotTag:     snapshotTag,
			Location:        []backupspec.Location{loc},
		}
		testTmpShadowedManifest(t, w, target)
	})

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

// testTmpShadowedManifest tests that 1-1-restore ignores shadowed temporary
// manifests when listing. It also validates, that it fails validation
// when not shadowed temporary manifest is encountered.
// This test helper assumes that a backup was already executed.
func testTmpShadowedManifest(t *testing.T, w *worker, target Target) {
	t.Helper()

	ctx := context.Background()
	location := target.Location[0]
	host := w.client.Config().Hosts[0]
	manifests, _, _, _ := listBucketFiles(t, ctx, w.client, location)
	// Find already uploaded manifest
	var manifestInfo backupspec.ManifestInfo
	for _, m := range manifests {
		if !strings.Contains(m, target.SnapshotTag) {
			continue
		}
		mi := backupspec.ManifestInfo{}
		if err := mi.ParsePath(m); err != nil {
			t.Fatal(err)
		}
		if mi.Temporary {
			continue
		}
		mi.Location = location
		manifestInfo = mi
		break
	}
	if manifestInfo.SnapshotTag != target.SnapshotTag {
		t.Fatalf("Manifest for snapshot tag %s not found", target.SnapshotTag)
	}
	// Inject unshadowed temporary manifest
	temporaryManifest := manifestInfo
	temporaryManifest.NodeID = uuid.MustRandom().String()
	temporaryManifest.Temporary = true
	if err := w.client.RclonePut(ctx, host, location.RemotePath(temporaryManifest.Path()), bytes.NewBufferString("broken manifest")); err != nil {
		t.Fatal(err)
	}
	// Expect it to cause validation error
	if _, _, err := w.getAllSnapshotManifestsAndTargetHosts(ctx, target); err == nil {
		t.Fatal("Expected unshadowed temporary manifest to fail 1-1-restore target validation")
	}
	// Replace unshadowed temporary manifest with a shadowed one.
	if err := w.client.RcloneDeleteFile(ctx, host, location.RemotePath(temporaryManifest.Path())); err != nil {
		t.Fatal(err)
	}
	shadowed := manifestInfo
	shadowed.Temporary = true
	if err := w.client.RclonePut(ctx, host, location.RemotePath(shadowed.Path()), bytes.NewBufferString("broken manifest")); err != nil {
		t.Fatal(err)
	}
	// Expect shadowed temporary manifest to not cause problems and not be returned
	got, _, err := w.getAllSnapshotManifestsAndTargetHosts(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range got {
		if m.Temporary {
			t.Fatalf("Expected shadowed temporary manifest not to be returned: %s", m.Path())
		}
	}
}

func listBucketFiles(t *testing.T, ctx context.Context, client *scyllaclient.Client, location backupspec.Location) (manifests, schemas, files, scyllaManifests []string) {
	t.Helper()

	host := client.Config().Hosts[0]
	opts := &scyllaclient.RcloneListDirOpts{
		Recurse:   true,
		FilesOnly: true,
	}
	allFiles, err := client.RcloneListDir(ctx, host, location.RemotePath(""), opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range allFiles {
		switch {
		case strings.HasPrefix(f.Path, ".permission-check"):
			// Ignore permission check leftover.
		case strings.HasPrefix(f.Path, "backup/meta"):
			manifests = append(manifests, f.Path)
		case strings.HasPrefix(f.Path, "backup/schema"):
			schemas = append(schemas, f.Path)
		case strings.HasPrefix(f.Path, "backup/sst") && strings.HasSuffix(f.Path, backupspec.ScyllaManifest):
			scyllaManifests = append(scyllaManifests, f.Path)
		case strings.HasPrefix(f.Path, "backup/sst"):
			files = append(files, f.Path)
		default:
			t.Fatalf("Unexpected file type in backup dir: %s", f.Path)
		}
	}
	return
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

	managerSession := db.CreateScyllaManagerDBSession(t)
	clusterSession := db.CreateSession(t, sc)

	w := &worker{
		managerSession: managerSession,
		client:         sc,
		clusterSession: clusterSession,
		sessionFunc: func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return db.CreateSession(t, sc), nil
		},
		logger:  log.NopLogger,
		metrics: metrics.NewOne2OneRestoreMetrics(),
	}
	return w, hrt
}
