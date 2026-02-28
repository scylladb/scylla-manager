// Copyright (C) 2026 ScyllaDB

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
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 1},
		"h2": {shardCnt: 2},
		"h3": {shardCnt: 3},
	}

	bd := newBatchDispatcher(workload, 1, hostInfo, locationInfo, MethodAuto)

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
		// All sstables in this test have integer based ID, so even with MethodAuto,
		// all batches should be restored with MethodRclone.
		if b.method != MethodRclone {
			t.Errorf("Expected method %q, got %q", MethodRclone, b.method)
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

// TestBatchDispatcherHostWithoutNativeRestore validates that when host doesn't support
// native restore, batches use --batch-size based batching and rclone restore method.
func TestBatchDispatcherHostWithoutNativeRestore(t *testing.T) {
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
			Size:             100,
			SSTables: []RemoteSSTable{
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 30},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 25},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 20},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 15},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 10},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)
	locationInfo := []LocationInfo{
		{
			Location: l,
			DCHosts:  map[string][]string{"dc1": {"h1"}},
		},
	}
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 2, nativeRestoreSupport: false},
	}

	bd := newBatchDispatcher(workload, 1, hostInfo, locationInfo, MethodAuto)

	// With --batch-size=1 and #shard=2, first batch contains 2 sstables
	b, ok := bd.dispatchBatch("h1")
	if !ok {
		t.Fatal("Expected batch to be dispatched")
	}
	if len(b.SSTables) != 2 {
		t.Errorf("Expected 2 sstables in first batch, got %d", len(b.SSTables))
	}
	if b.Size != 55 {
		t.Errorf("Expected batch size 55, got %d", b.Size)
	}
	if b.method != MethodRclone {
		t.Errorf("Expected method %q, got %q", MethodRclone, b.method)
	}
	bd.ReportSuccess(b)

	// Second batch initially has 2 sstables, but is extended
	// so extended to include all 3 remaining sstables.
	b, ok = bd.dispatchBatch("h1")
	if !ok {
		t.Fatal("Expected batch to be dispatched")
	}
	if len(b.SSTables) != 3 {
		t.Errorf("Expected 3 sstables in second batch, got %d", len(b.SSTables))
	}
	if b.Size != 45 {
		t.Errorf("Expected batch size 45, got %d", b.Size)
	}
	if b.method != MethodRclone {
		t.Errorf("Expected method %q, got %q", MethodRclone, b.method)
	}
	bd.ReportSuccess(b)

	_, ok = bd.dispatchBatch("h1")
	if ok {
		t.Fatal("Expected no more batches")
	}
	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}
}

// TestBatchDispatcherIntegerID validates that sstables with
// integer based IDs use 5% limit when maxBatchSize is used.
func TestBatchDispatcherIntegerID(t *testing.T) {
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
			Size:             1000,
			SSTables: []RemoteSSTable{
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 200},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 200},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 200},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 200},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 200},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)
	locationInfo := []LocationInfo{
		{
			Location: l,
			DCHosts:  map[string][]string{"dc1": {"h1"}},
		},
	}
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 2, nativeRestoreSupport: true},
	}

	bd := newBatchDispatcher(workload, maxBatchSize, hostInfo, locationInfo, MethodAuto)

	// With 5% limit, first batch contains 2 sstables
	b, ok := bd.dispatchBatch("h1")
	if !ok {
		t.Fatal("Expected batch to be dispatched")
	}
	if len(b.SSTables) != 2 {
		t.Errorf("Expected 2 sstables in first batch, got %d", len(b.SSTables))
	}
	if b.Size != 400 {
		t.Errorf("Expected batch size 400, got %d", b.Size)
	}
	if b.method != MethodRclone {
		t.Errorf("Expected method %q, got %q", MethodRclone, b.method)
	}
	bd.ReportSuccess(b)

	// Second batch initially has 2 sstables, but is extended
	// so extended to include all 3 remaining sstables.
	b, ok = bd.dispatchBatch("h1")
	if !ok {
		t.Fatal("Expected batch to be dispatched")
	}
	if len(b.SSTables) != 3 {
		t.Errorf("Expected 3 sstables in second batch, got %d", len(b.SSTables))
	}
	if b.Size != 600 {
		t.Errorf("Expected batch size 600, got %d", b.Size)
	}
	if b.method != MethodRclone {
		t.Errorf("Expected method %q, got %q", MethodRclone, b.method)
	}
	bd.ReportSuccess(b)

	_, ok = bd.dispatchBatch("h1")
	if ok {
		t.Fatal("Expected no more batches")
	}
	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}
}

// TestBatchDispatcherMultiHostNative validates that expected shard workload
// is correctly computed across multiple hosts and that native restore batches
// aim for 100% of expected node workload.
func TestBatchDispatcherMultiHostNative(t *testing.T) {
	l := backupspec.Location{
		Provider: "s3",
		Path:     "l",
	}

	// Total size = 500, total shards = 5, expected shard workload = 100.
	// h1, shards = 2, expected workload = 200.
	// h2, shards = 3, expected workload = 300.
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
			Size:             500,
			SSTables: []RemoteSSTable{
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 100},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 90},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 80},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 70},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 60},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 50},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 50},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)
	locationInfo := []LocationInfo{
		{
			Location: l,
			DCHosts:  map[string][]string{"dc1": {"h1", "h2"}},
		},
	}
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 2, nativeRestoreSupport: true},
		"h2": {shardCnt: 3, nativeRestoreSupport: true},
	}

	bd := newBatchDispatcher(workload, 1, hostInfo, locationInfo, MethodAuto)

	// Host h1 with 2 shards gets the first batch.
	// SSTables sorted descending: [100, 90, 80, 70, 60, 50, 50].
	// Since 2 first sstables are smaller than expected workload,
	// the next 2 are also added to the batch. This might seem like
	// a poor batching strategy, but that's because sstable count
	// is relatively small to the shard count, which is not the case
	// in the real life scenarios.
	b1, ok := bd.dispatchBatch("h1")
	if !ok {
		t.Fatal("Expected batch for h1")
	}
	if b1.method != MethodNative {
		t.Errorf("Expected method %q, got %q", MethodNative, b1.method)
	}
	if len(b1.SSTables) != 4 {
		t.Errorf("Expected 4 sstables for h1, got %d", len(b1.SSTables))
	}
	if b1.Size != 340 {
		t.Errorf("Expected batch size 340, got %d", b1.Size)
	}
	bd.ReportSuccess(b1)

	// Host h2 with 3 shards gets the second batch.
	// Remaining SSTables: [60, 50, 50].
	// It takes all 3 remaining sstables.
	b2, ok := bd.dispatchBatch("h2")
	if !ok {
		t.Fatal("Expected batch for h2")
	}
	if b2.method != MethodNative {
		t.Errorf("Expected method %q, got %q", MethodNative, b2.method)
	}
	if len(b2.SSTables) != 3 {
		t.Errorf("Expected 3 SSTables for h2, got %d", len(b2.SSTables))
	}
	if b2.Size != 160 {
		t.Errorf("Expected batch size 160, got %d", b2.Size)
	}
	bd.ReportSuccess(b2)

	_, ok = bd.dispatchBatch("h1")
	if ok {
		t.Fatal("Expected no more batches for h1")
	}
	_, ok = bd.dispatchBatch("h2")
	if ok {
		t.Fatal("Expected no more batches for h2")
	}
	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}
}

// TestBatchDispatcherMixedSSTables validates that when RemoteSSTableDir
// contains both UUID and integer ID sstables, they are grouped into
// separate batches with different types and restore methods.
func TestBatchDispatcherMixedSSTables(t *testing.T) {
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
			Size:             100,
			SSTables: []RemoteSSTable{
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 30},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}}, Size: 20},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 30},
				{SSTable: SSTable{ID: sstable.ID{Type: sstable.IntegerID}}, Size: 20},
			},
		},
	}

	workload := aggregateWorkload(rawWorkload)
	locationInfo := []LocationInfo{
		{
			Location: l,
			DCHosts:  map[string][]string{"dc1": {"h1"}},
		},
	}
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 1, nativeRestoreSupport: true},
	}

	bd := newBatchDispatcher(workload, 1, hostInfo, locationInfo, MethodAuto)

	var nativeBatches, rcloneBatches int
	for {
		b, ok := bd.dispatchBatch("h1")
		if !ok {
			break
		}
		switch b.method {
		case MethodNative:
			nativeBatches++
			for _, sst := range b.SSTables {
				if sst.ID.Type == sstable.IntegerID {
					t.Error("Native batch contains integer based sstable")
				}
			}
		case MethodRclone:
			rcloneBatches++
			for _, sst := range b.SSTables {
				if sst.ID.Type == sstable.UUID {
					t.Error("Rclone batch contains UUID sstable")
				}
			}
		default:
			t.Errorf("Unexpected method %q", b.method)
		}
		bd.ReportSuccess(b)
	}

	if nativeBatches != 1 {
		t.Errorf("Expected single native batch, got %d", nativeBatches)
	}
	if rcloneBatches != 2 {
		t.Errorf("Expected 2 rclone batches, got %d", rcloneBatches)
	}
	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}
}

// TestBatchDispatcherNativeFallback validates that the native batch
// fallback mechanism limits per-node workload imbalance.
func TestBatchDispatcherNativeFallback(t *testing.T) {
	generateSSTables := func(n int, size int64) []RemoteSSTable {
		ssts := make([]RemoteSSTable, n)
		for i := range ssts {
			ssts[i] = RemoteSSTable{
				SSTable: SSTable{ID: sstable.ID{Type: sstable.UUID}},
				Size:    size,
			}
		}
		return ssts
	}

	l := backupspec.Location{Provider: "s3", Path: "l"}
	mi := &backupspec.ManifestInfo{
		Location: l,
		DC:       "dc1",
	}
	tn := TableName{Keyspace: "ks1", Table: "t1"}
	// Scenario: 3 nodes 1 shard each, 5 RemoteSSTableDirs,
	// each with 70 UUID sstables of 10 bytes.
	// Total size = 3500, expected node workload = 1166.
	rawWorkload := []RemoteDirWorkload{
		{
			ManifestInfo:     mi,
			TableName:        tn,
			RemoteSSTableDir: "a",
			Size:             700,
			SSTables:         generateSSTables(70, 10),
		},
		{
			ManifestInfo:     mi,
			TableName:        tn,
			RemoteSSTableDir: "b",
			Size:             700,
			SSTables:         generateSSTables(70, 10),
		},
		{
			ManifestInfo:     mi,
			TableName:        tn,
			RemoteSSTableDir: "c",
			Size:             700,
			SSTables:         generateSSTables(70, 10),
		},
		{
			ManifestInfo:     mi,
			TableName:        tn,
			RemoteSSTableDir: "d",
			Size:             700,
			SSTables:         generateSSTables(70, 10),
		},
		{
			ManifestInfo:     mi,
			TableName:        tn,
			RemoteSSTableDir: "e",
			Size:             700,
			SSTables:         generateSSTables(70, 10),
		},
	}

	workload := aggregateWorkload(rawWorkload)
	hosts := []string{"h1", "h2", "h3"}
	locationInfo := []LocationInfo{
		{
			Location: l,
			DCHosts: map[string][]string{
				"dc1": hosts,
			},
		},
	}
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 1, nativeRestoreSupport: true},
		"h2": {shardCnt: 1, nativeRestoreSupport: true},
		"h3": {shardCnt: 1, nativeRestoreSupport: true},
	}

	bd := newBatchDispatcher(workload, 1, hostInfo, locationInfo, MethodAuto)
	// Simulate same per-shard speed by always dispatching next batch
	// to the node that has the least total assigned bytes.
	hostAssignedBytes := make(map[string]int64)
	leastLoadedHost := func() string {
		least := hosts[0]
		for _, h := range hosts[1:] {
			if hostAssignedBytes[h] < hostAssignedBytes[least] {
				least = h
			}
		}
		return least
	}

	for {
		h := leastLoadedHost()
		b, ok := bd.dispatchBatch(h)
		if !ok {
			break
		}
		if b.method != MethodNative {
			t.Errorf("Expected method %q, got %q", MethodNative, b.method)
		}
		hostAssignedBytes[h] += b.Size
		bd.ReportSuccess(b)
	}

	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected all sstables to be batched: %s", err)
	}

	var total int64
	for _, v := range hostAssignedBytes {
		total += v
	}
	if total != 3500 {
		t.Fatalf("Expected total bytes 3500, got %d", total)
	}

	// Validate that the fallback mechanism prevents large imbalance.
	// Without fallback: h1=1400, h2=1400, h3=700.
	// With fallback:    h1≈1170, h2≈1160, h3≈1170.
	// Assert that the difference is below 10% of expected node workload,
	// which is only possible with the fallback.
	minBytes := total
	maxBytes := int64(0)
	for _, v := range hostAssignedBytes {
		minBytes = min(minBytes, v)
		maxBytes = max(maxBytes, v)
	}
	expectedNodeWorkload := total / int64(len(hosts))
	if maxBytes-minBytes > expectedNodeWorkload/10 {
		t.Errorf("Workload imbalance too high: max=%d, min=%d", maxBytes, minBytes)
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
	hostInfo := map[string]hostBatchInfo{
		"h1": {shardCnt: 1, nativeRestoreSupport: false},
	}

	bd := newBatchDispatcher(workload, batchSize, hostInfo, locationHosts, MethodAuto)

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
