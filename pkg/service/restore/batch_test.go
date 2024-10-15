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
	workload := []LocationWorkload{
		{
			Location: l1,
			Size:     170,
			Tables: []TableWorkload{
				{
					Size: 60,
					RemoteDirs: []RemoteDirWorkload{
						{
							RemoteSSTableDir: "a",
							Size:             20,
							SSTables: []RemoteSSTable{
								{Size: 5},
								{Size: 15},
							},
						},
						{
							RemoteSSTableDir: "e",
							Size:             10,
							SSTables: []RemoteSSTable{
								{Size: 2},
								{Size: 4},
								{Size: 4},
							},
						},
						{
							RemoteSSTableDir: "b",
							Size:             30,
							SSTables: []RemoteSSTable{
								{Size: 10},
								{Size: 20},
							},
						},
					},
				},
				{
					Size: 110,
					RemoteDirs: []RemoteDirWorkload{
						{
							RemoteSSTableDir: "c",
							Size:             110,
							SSTables: []RemoteSSTable{
								{Size: 50},
								{Size: 60},
							},
						},
					},
				},
			},
		},
		{
			Location: l2,
			Size:     200,
			Tables: []TableWorkload{
				{
					Size: 200,
					RemoteDirs: []RemoteDirWorkload{
						{
							RemoteSSTableDir: "d",
							Size:             200,
							SSTables: []RemoteSSTable{
								{Size: 110},
								{Size: 90},
							},
						},
					},
				},
			},
		},
	}
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
	}{
		{host: "h1", ok: true, dir: "c", size: 60, count: 1},
		{host: "h1", ok: true, dir: "c", size: 50, count: 1},
		{host: "h2", ok: true, dir: "b", size: 30, count: 2},
		{host: "h3", ok: true, dir: "d", size: 200, count: 2},
		{host: "h3", ok: false},
		{host: "h2", ok: true, dir: "a", size: 20, count: 2},
		{host: "h2", ok: true, dir: "e", size: 10, count: 3}, // batch extended with leftovers < shard_cnt
		{host: "h1", ok: false},
		{host: "h2", ok: false},
	}

	for _, step := range scenario {
		b, ok := bd.dispatchBatch(step.host)
		if ok != step.ok {
			t.Fatalf("Step: %+v, expected ok=%v, got ok=%v", step, step.ok, ok)
		}
		if ok == false {
			continue
		}
		if b.RemoteSSTableDir != step.dir {
			t.Fatalf("Step: %+v, expected dir=%v, got dir=%v", step, step.dir, b.RemoteSSTableDir)
		}
		if b.Size != step.size {
			t.Fatalf("Step: %+v, expected size=%v, got size=%v", step, step.size, b.Size)
		}
		if len(b.SSTables) != step.count {
			t.Fatalf("Step: %+v, expected count=%v, got count=%v", step, step.count, len(b.SSTables))
		}
		bd.ReportSuccess(b)
	}

	if err := bd.ValidateAllDispatched(); err != nil {
		t.Fatalf("Expected sstables to be batched: %s", err)
	}
}
