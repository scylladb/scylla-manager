// Copyright (C) 2024 ScyllaDB

package restore

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
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

	locationHosts := map[backupspec.Location][]string{
		l1: {"h1", "h2"},
		l2: {"h3"},
	}
	hostToShard := map[string]uint{
		"h1": 1,
		"h2": 2,
		"h3": 3,
	}

	bd := newBatchDispatcher(workload, 1, hostToShard, locationHosts)

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
