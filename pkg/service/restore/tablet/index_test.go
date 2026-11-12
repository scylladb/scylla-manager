// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"bytes"
	"net/netip"
	"path"
	"reflect"
	"slices"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/topology"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

// tabletAwareBackupVersion is a version satisfying scyllaTabletAwareBackupSupport.
const tabletAwareBackupVersion = "2026.1.1"

func TestCheckCluster(t *testing.T) {
	const endpoint = "192.168.200.99"
	withEndpoint := configcache.NodeConfig{NodeInfo: &scyllaclient.NodeInfo{
		RcloneBackendConfig:    models.NodeInfoRcloneBackendConfig{S3: models.NodeInfoRcloneBackendConfigS3{Endpoint: endpoint}},
		ObjectStorageEndpoints: map[string]models.ObjectStorageEndpoint{endpoint: {Name: endpoint}},
	}}
	withoutEndpoint := configcache.NodeConfig{NodeInfo: &scyllaclient.NodeInfo{}}
	n1, n2 := netip.MustParseAddr("192.168.200.11"), netip.MustParseAddr("192.168.200.12")
	s3 := []backupspec.Location{{Provider: backupspec.S3, Path: "bucket"}}
	singleDC := testTopology(dcRack{dc: "dc1", rack: "r1"})

	testCases := []struct {
		name       string
		nodeConfig map[netip.Addr]configcache.NodeConfig
		locations  []backupspec.Location
		dcMappings map[string]string
		targetTopo topology.ClusterTopology
		err        bool
	}{
		{
			name:       "compatible",
			nodeConfig: map[netip.Addr]configcache.NodeConfig{n1: withEndpoint, n2: withEndpoint},
			locations:  s3,
			targetTopo: singleDC,
		},
		{
			name:       "dc mapping keeping dc name",
			nodeConfig: map[netip.Addr]configcache.NodeConfig{n1: withEndpoint},
			locations:  s3,
			dcMappings: map[string]string{"dc1": "dc1"},
			targetTopo: singleDC,
		},
		{
			name:       "node without object storage endpoint",
			nodeConfig: map[netip.Addr]configcache.NodeConfig{n1: withEndpoint, n2: withoutEndpoint},
			locations:  s3,
			targetTopo: singleDC,
			err:        true,
		},
		{
			name:       "dc mapping changing dc name",
			nodeConfig: map[netip.Addr]configcache.NodeConfig{n1: withEndpoint},
			locations:  s3,
			dcMappings: map[string]string{"dc1": "dc2"},
			targetTopo: singleDC,
			err:        true,
		},
		{
			name:       "multi-dc target cluster",
			nodeConfig: map[netip.Addr]configcache.NodeConfig{n1: withEndpoint},
			locations:  s3,
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}, dcRack{dc: "dc2", rack: "r1"}),
			err:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := &IndexWorker{
				logger:         log.NopLogger,
				targetTopology: tc.targetTopo,
			}

			err := w.CheckCluster(tc.nodeConfig, tc.locations, tc.dcMappings)
			if tc.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.err && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestIndex(t *testing.T) {
	const (
		dc             = "dc1"
		version        = "v1"
		scyllaManifest = "manifest"
	)
	// Keep manifest index files in the test temp dir.
	// When indexing tablet aware restore workload, worker operates on
	// manifests with Index saved on disk, so we need to replicate that
	// for this test.
	t.Setenv("TMPDIR", t.TempDir())

	tab := table.CQLTable{Keyspace: "ks", Name: "tab"}
	vnodeTab := table.CQLTable{Keyspace: "vnode_ks", Name: "tab"}
	missingTab := table.CQLTable{Keyspace: "ks", Name: "missing_tab"}
	fm := backupspec.FilesMeta{
		Keyspace:        tab.Keyspace,
		Table:           tab.Name,
		ReplicationType: string(scyllaclient.ReplicationTablet),
		Version:         version,
		Files:           []string{"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Data.db", "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-TOC.txt"},
		Size:            10,
		ScyllaManifests: []string{scyllaManifest},
	}
	vnodeFM := fm
	vnodeFM.Keyspace = vnodeTab.Keyspace
	vnodeFM.ReplicationType = string(scyllaclient.ReplicationVnode)
	m1 := testManifestWithIndex(t, dc, "r1", "n1", fm, vnodeFM)
	m2 := testManifestWithIndex(t, dc, "r2", "n2", fm, vnodeFM)
	tabWorkload := Workload{
		tab: {
			Table: tab,
			Datacenters: map[string]DcMeta{
				dc: {
					DC:       dc,
					Provider: m1.Location.Provider,
					Bucket:   m1.Location.Path,
					RemoteManifests: []string{
						path.Join(m1.SSTableVersionDir(tab.Keyspace, tab.Name, version), scyllaManifest),
						path.Join(m2.SSTableVersionDir(tab.Keyspace, tab.Name, version), scyllaManifest),
					},
				},
			},
			FileCnt: 4,
			Size:    20,
		},
	}

	testCases := []struct {
		name     string
		tables   []table.CQLTable
		expected Workload
		err      bool
	}{
		{
			name:     "table found in manifests",
			tables:   []table.CQLTable{tab},
			expected: tabWorkload,
		},
		{
			name:     "table not eligible for tablet aware restore",
			tables:   []table.CQLTable{tab, vnodeTab},
			expected: tabWorkload,
		},
		{
			name:   "table not found in manifests",
			tables: []table.CQLTable{tab, missingTab},
			err:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := &IndexWorker{
				logger:         log.NopLogger,
				targetTopology: testTopology(dcRack{dc: dc, rack: "r1"}, dcRack{dc: dc, rack: "r2"}),
				isTablet:       func(string) bool { return true },
			}

			workload, err := w.Index(t.Context(), maps.SetFromSlice(tc.tables), []backupspec.ManifestInfoWithContent{m1, m2})
			if tc.err {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			// Manifests are indexed in parallel
			for _, tm := range workload {
				for _, dcm := range tm.Datacenters {
					slices.Sort(dcm.RemoteManifests)
				}
			}
			if !reflect.DeepEqual(workload, tc.expected) {
				t.Fatalf("expected workload %v, got %v", tc.expected, workload)
			}
		})
	}
}

func TestCheckManifestsCompatibility(t *testing.T) {
	testCases := []struct {
		name       string
		manifests  []backupspec.ManifestInfoWithContent
		targetTopo topology.ClusterTopology
		ok         bool
		err        bool
	}{
		{
			name: "compatible",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", tabletAwareBackupVersion),
				testManifest("dc1", "r2", "n2", tabletAwareBackupVersion),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}, dcRack{dc: "dc1", rack: "r2"}),
			ok:         true,
		},
		{
			name: "manifest with too old scylla version",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", tabletAwareBackupVersion),
				testManifest("dc1", "r2", "n2", "2025.4.0"),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}, dcRack{dc: "dc1", rack: "r2"}),
			ok:         false,
		},
		{
			name: "manifest without scylla version",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", ""),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}),
			ok:         false,
		},
		{
			name: "invalid manifest scylla version",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", "not-a-version"),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}),
			err:        true,
		},
		{
			name: "backup DC missing in target topology",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", tabletAwareBackupVersion),
				testManifest("dc2", "r1", "n2", tabletAwareBackupVersion),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}),
			ok:         false,
		},
		{
			name: "backup rack missing in target topology",
			manifests: []backupspec.ManifestInfoWithContent{
				testManifest("dc1", "r1", "n1", tabletAwareBackupVersion),
				testManifest("dc1", "r2", "n2", tabletAwareBackupVersion),
			},
			targetTopo: testTopology(dcRack{dc: "dc1", rack: "r1"}, dcRack{dc: "dc1", rack: "r3"}),
			ok:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := &IndexWorker{
				logger:         log.NopLogger,
				targetTopology: tc.targetTopo,
			}

			ok, err := w.checkManifestsCompatibility(t.Context(), tc.manifests)
			if tc.err {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v", tc.ok, ok)
			}
		})
	}
}

func TestCheckFilesMetaCompatibility(t *testing.T) {
	const (
		uuidSSTableData    = "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Data.db"
		uuidSSTableTOC     = "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-TOC.txt"
		integerSSTableData = "me-7-big-Data.db"
	)

	testCases := []struct {
		name     string
		fm       backupspec.FilesMeta
		isTablet bool
		ok       bool
		err      bool
	}{
		{
			name: "compatible",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationTablet),
				Files:           []string{uuidSSTableData, uuidSSTableTOC},
				ScyllaManifests: []string{"manifest"},
			},
			isTablet: true,
			ok:       true,
		},
		{
			name: "no scylla manifests",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationTablet),
				Files:           []string{uuidSSTableData},
			},
			isTablet: true,
			ok:       false,
		},
		{
			name: "vnode replication in backup",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationVnode),
				Files:           []string{uuidSSTableData},
				ScyllaManifests: []string{"manifest"},
			},
			isTablet: true,
			ok:       false,
		},
		{
			name: "vnode replication in target cluster",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationTablet),
				Files:           []string{uuidSSTableData},
				ScyllaManifests: []string{"manifest"},
			},
			isTablet: false,
			ok:       false,
		},
		{
			name: "integer based sstable ID",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationTablet),
				Files:           []string{uuidSSTableData, integerSSTableData},
				ScyllaManifests: []string{"manifest"},
			},
			isTablet: true,
			ok:       false,
		},
		{
			name: "unparsable sstable name",
			fm: backupspec.FilesMeta{
				Keyspace:        "ks",
				Table:           "tab",
				ReplicationType: string(scyllaclient.ReplicationTablet),
				Files:           []string{"garbage"},
				ScyllaManifests: []string{"manifest"},
			},
			isTablet: true,
			err:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := &IndexWorker{
				logger:   log.NopLogger,
				isTablet: func(string) bool { return tc.isTablet },
			}

			ok, err := w.checkFilesMetaCompatibility(t.Context(), tc.fm)
			if tc.err {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v", tc.ok, ok)
			}
		})
	}
}

func testManifest(dc, rack, nodeID, scyllaVersion string) backupspec.ManifestInfoWithContent {
	m := backupspec.NewManifestInfoWithContent()
	m.ManifestInfo.DC = dc
	m.ManifestInfo.NodeID = nodeID
	m.ManifestContentWithIndex.Rack = rack
	m.ManifestContentWithIndex.ScyllaVersion = scyllaVersion
	return m
}

// testManifestWithIndex is testManifest with index saved in both manifest Index and on disk.
// This allows both Index access patterns: directly from memory, iterator from file.
func testManifestWithIndex(t *testing.T, dc, rack, nodeID string, index ...backupspec.FilesMeta) backupspec.ManifestInfoWithContent {
	t.Helper()

	m := testManifest(dc, rack, nodeID, tabletAwareBackupVersion)
	m.Location = backupspec.Location{Provider: backupspec.S3, Path: "bucket"}
	m.Index = index
	buf := &bytes.Buffer{}
	if err := m.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := m.Read(buf); err != nil {
		t.Fatal(err)
	}
	return m
}

type dcRack struct {
	dc   string
	rack string
}

func testTopology(dcRacks ...dcRack) topology.ClusterTopology {
	return topology.BuildClusterTopology(func(yield func(string, string) bool) {
		for _, dr := range dcRacks {
			if !yield(dr.dc, dr.rack) {
				return
			}
		}
	})
}
