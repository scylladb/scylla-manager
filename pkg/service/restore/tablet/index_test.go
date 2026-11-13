// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"net/netip"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
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
