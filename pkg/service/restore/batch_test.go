// Copyright (C) 2024 ScyllaDB

package restore

import (
	"maps"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
)

func TestBatchDispatcher(t *testing.T) {
	l1 := backupspec.Location{
		Provider: "s3",
		Path:     "l1",
	}
	l2 := backupspec.Location{
		Provider: "s3",
		Path:     "l2",
	}

	rawWorkload := []RemoteDirWorkload{
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l1,
				DC:       "dc1",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t1",
			},
			RemoteSSTableDir: "a",
			Size:             20,
			SSTables: []RemoteSSTable{
				{Size: 5},
				{Size: 15},
			},
		},
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l1,
				DC:       "dc1",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t1",
			},
			RemoteSSTableDir: "e",
			Size:             10,
			SSTables: []RemoteSSTable{
				{Size: 2},
				{Size: 4},
				{Size: 4},
			},
		},
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l1,
				DC:       "dc2",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t1",
			},
			RemoteSSTableDir: "b",
			Size:             30,
			SSTables: []RemoteSSTable{
				{Size: 10},
				{Size: 20},
			},
		},
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l1,
				DC:       "dc1",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t2",
			},
			RemoteSSTableDir: "c",
			Size:             110,
			SSTables: []RemoteSSTable{
				{Size: 50},
				{Size: 60},
			},
		},
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l2,
				DC:       "dc3",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t2",
			},
			RemoteSSTableDir: "d",
			Size:             200,
			SSTables: []RemoteSSTable{
				{Size: 110},
				{Size: 90},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)

	hostToShard := map[string]uint{
		"h1": 1,
		"h2": 2,
		"h3": 3,
	}

	locationInfo := []LocationInfo{
		{
			Location: l1,
			DCHosts: map[string][]string{
				"dc1": {"h1", "h2"},
				"dc2": {"h1", "h2"},
			},
		},
		{
			Location: l2,
			DCHosts: map[string][]string{
				"dc3": {"h3"},
			},
		},
	}

	bd := newBatchDispatcher(workload, 1, hostToShard, locationInfo)

	scenario := []struct {
		host  string
		ok    bool
		dir   string
		size  int64
		count int
		err   bool
	}{
		{host: "h1", ok: true, dir: "c", size: 60, count: 1},
		{host: "h1", ok: true, dir: "c", size: 50, count: 1, err: true},
		{host: "h1", ok: true, dir: "b", size: 20, count: 1}, // host retry in different dc
		{host: "h2", ok: true, dir: "c", size: 50, count: 1}, // batch retry
		{host: "h1", ok: true, dir: "b", size: 10, count: 1, err: true},
		{host: "h1"}, // already failed in all dcs
		{host: "h2", ok: true, dir: "b", size: 10, count: 1}, // batch retry
		{host: "h2", ok: true, dir: "b", size: 30, count: 2},
		{host: "h3", ok: true, dir: "d", size: 200, count: 2},
		{host: "h3"},
		{host: "h2", ok: true, dir: "a", size: 20, count: 2},
		{host: "h2", ok: true, dir: "e", size: 10, count: 3}, // batch extended with leftovers < shard_cnt
		{host: "h1"},
		{host: "h2"},
	}

	for _, step := range scenario {
		// use dispatchBatch instead of DispatchBatch because
		// we don't want to hang here.
		b, ok := bd.dispatchBatch(step.host)
		if ok != step.ok {
			t.Errorf("Expected %v, got %#v", step, b)
		}
		if ok == false {
			return
		}
		if b.RemoteSSTableDir != step.dir || b.Size != step.size || len(b.SSTables) != step.count {
			t.Errorf("Expected %v, got %#v", step, b)
		}
		if step.err {
			if err := bd.ReportFailure(step.host, b); err != nil {
				t.Fatal(err)
			}
		} else {
			bd.ReportSuccess(b)
		}
	}

	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected sstables to be batched: %s", err)
	}
}

func TestGetHostDCAccess(t *testing.T) {
	testCases := []struct {
		name string

		locationInfo []LocationInfo

		expected map[string][]string
	}{
		{
			name: "one location with one DC",
			locationInfo: []LocationInfo{
				{
					DCHosts: map[string][]string{
						"dc1": {"host1", "host2"},
					},
				},
			},
			expected: map[string][]string{
				"host1": {"dc1"},
				"host2": {"dc1"},
			},
		},
		{
			name: "one location with two DC's",
			locationInfo: []LocationInfo{
				{
					DCHosts: map[string][]string{
						"dc1": {"host1"},
						"dc2": {"host2"},
					},
				},
			},
			expected: map[string][]string{
				"host1": {"dc1"},
				"host2": {"dc2"},
			},
		},
		{
			name: "one location with two DC's, more nodes",
			locationInfo: []LocationInfo{
				{
					DCHosts: map[string][]string{
						"dc1": {"host1", "host2"},
						"dc2": {"host3", "host4"},
					},
				},
			},
			expected: map[string][]string{
				"host1": {"dc1"},
				"host2": {"dc1"},
				"host3": {"dc2"},
				"host4": {"dc2"},
			},
		},
		{
			name: "two locations with one DC each",
			locationInfo: []LocationInfo{
				{
					DCHosts: map[string][]string{
						"dc1": {"host1"},
					},
				},
				{
					DCHosts: map[string][]string{
						"dc2": {"host2"},
					},
				},
			},
			expected: map[string][]string{
				"host1": {"dc1"},
				"host2": {"dc2"},
			},
		},
		{
			name: "two locations with one DC each, but hosts maps to all dcs",
			locationInfo: []LocationInfo{
				{
					DCHosts: map[string][]string{
						"dc1": {"host1", "host2"},
					},
				},
				{
					DCHosts: map[string][]string{
						"dc2": {"host1", "host2"},
					},
				},
			},
			expected: map[string][]string{
				"host1": {"dc1", "dc2"},
				"host2": {"dc1", "dc2"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := getHostDCAccess(tc.locationInfo)
			if diff := cmp.Diff(actual, tc.expected); diff != "" {
				t.Fatalf("Actual != Expected: %s", diff)
			}
		})
	}
}

func TestBatchDispatcherType(t *testing.T) {
	l := backupspec.Location{
		Provider: "s3",
		Path:     "l",
	}

	rawWorkload := []RemoteDirWorkload{
		{
			ManifestInfo: &backupspec.ManifestInfo{
				Location: l,
				DC:       "dc1",
			},
			TableName: TableName{
				Keyspace: "ks1",
				Table:    "t1",
			},
			RemoteSSTableDir: "a",
			Size:             7,
			SSTables: []RemoteSSTable{
				// Integer ID, not versioned
				{
					SSTable:   SSTable{ID: sstable.ID{Type: sstable.IntegerID}},
					Versioned: false,
					Size:      1,
				},
				{
					SSTable:   SSTable{ID: sstable.ID{Type: sstable.IntegerID}},
					Versioned: false,
					Size:      1,
				},
				{
					SSTable:   SSTable{ID: sstable.ID{Type: sstable.IntegerID}},
					Versioned: false,
					Size:      1,
				},
				{
					SSTable:   SSTable{ID: sstable.ID{Type: sstable.IntegerID}},
					Versioned: false,
					Size:      1,
				},
				// Integer ID, versioned
				{
					SSTable:   SSTable{ID: sstable.ID{Type: sstable.IntegerID}},
					Versioned: true,
					Size:      1,
				},
				// UUID, not versioned
				{
					SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}},
					Size:    1,
				},
				{
					SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}},
					Size:    1,
				},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)
	batchSize := 3
	locationHosts := []LocationInfo{
		{
			DCHosts: map[string][]string{
				"dc1": {"h1"},
			},
			Location: l,
		},
	}
	hostToShard := map[string]uint{
		"h1": 1,
	}

	bd := newBatchDispatcher(workload, batchSize, hostToShard, locationHosts)

	type batchTypeWithSSTableCnt struct {
		bt         batchType
		SSTableCnt int
	}

	// Describes how many batchTypeWithSSTableCnt are we expecting to encounter
	expected := map[batchTypeWithSSTableCnt]int{
		batchTypeWithSSTableCnt{
			bt:         batchType{IDType: sstable.IntegerID, Versioned: false},
			SSTableCnt: 3,
		}: 1,
		batchTypeWithSSTableCnt{
			bt:         batchType{IDType: sstable.IntegerID, Versioned: false},
			SSTableCnt: 1,
		}: 1,
		batchTypeWithSSTableCnt{
			bt:         batchType{IDType: sstable.IntegerID, Versioned: true},
			SSTableCnt: 1,
		}: 1,
		batchTypeWithSSTableCnt{
			bt:         batchType{IDType: sstable.UUID},
			SSTableCnt: 2,
		}: 1,
	}

	result := make(map[batchTypeWithSSTableCnt]int)
	for {
		b, ok := bd.dispatchBatch("h1")
		if !ok {
			break
		}
		result[batchTypeWithSSTableCnt{
			bt:         b.batchType,
			SSTableCnt: len(b.SSTables),
		}]++
		bd.ReportSuccess(b)
	}

	if !maps.Equal(expected, result) {
		t.Fatalf("Expected batches %v, got %v", expected, result)
	}
	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}
}
