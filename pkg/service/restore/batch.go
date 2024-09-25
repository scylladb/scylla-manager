// Copyright (C) 2024 ScyllaDB

package restore

import (
	"slices"
	"sync"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

type batchDispatcher struct {
	mu            sync.Mutex
	workload      []LocationWorkload
	batchSize     int
	hostShardCnt  map[string]uint
	locationHosts map[Location][]string
}

func newBatchDispatcher(workload []LocationWorkload, batchSize int, hostShardCnt map[string]uint, locationHosts map[Location][]string) *batchDispatcher {
	return &batchDispatcher{
		mu:            sync.Mutex{},
		workload:      workload,
		batchSize:     batchSize,
		hostShardCnt:  hostShardCnt,
		locationHosts: locationHosts,
	}
}

type batch struct {
	TableName
	*ManifestInfo

	RemoteSSTableDir string
	Size             int64
	SSTables         []RemoteSSTable
}

func (b batch) NotVersionedSSTables() []RemoteSSTable {
	var ssts []RemoteSSTable
	for _, sst := range b.SSTables {
		if !sst.Versioned {
			ssts = append(ssts, sst)
		}
	}
	return ssts
}

func (b batch) VersionedSSTables() []RemoteSSTable {
	var ssts []RemoteSSTable
	for _, sst := range b.SSTables {
		if sst.Versioned {
			ssts = append(ssts, sst)
		}
	}
	return ssts
}

func (b batch) VersionedSize() int64 {
	var size int64
	for _, sst := range b.SSTables {
		if sst.Versioned {
			size += sst.Size
		}
	}
	return size
}

func (b batch) IDs() []string {
	var ids []string
	for _, sst := range b.SSTables {
		ids = append(ids, sst.ID)
	}
	return ids
}

// ValidateAllDispatched returns error if not all sstables were dispatched.
func (b *batchDispatcher) ValidateAllDispatched() error {
	for _, lw := range b.workload {
		if lw.Size != 0 {
			for _, tw := range lw.Tables {
				if tw.Size != 0 {
					for _, dw := range tw.RemoteDirs {
						if dw.Size != 0 || len(dw.SSTables) != 0 {
							return errors.Errorf("expected all data to be restored, missing sstable ids from location %s table %s.%s: %v (%d bytes)",
								dw.Location, dw.Keyspace, dw.Table, dw.SSTables, dw.Size)
						}
					}
					return errors.Errorf("expected all data to be restored, missinng table from location %s: %s.%s (%d bytes)",
						tw.Location, tw.Keyspace, tw.Table, tw.Size)
				}
			}
			return errors.Errorf("expected all data to be restored, missinng location: %s (%d bytes)",
				lw.Location, lw.Size)
		}
	}
	return nil
}

// DispatchBatch batch to be restored or false when there is no more work to do.
func (b *batchDispatcher) DispatchBatch(host string) (batch, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	l := b.chooseLocation(host)
	if l == nil {
		return batch{}, false
	}
	t := b.chooseTable(l)
	if t == nil {
		return batch{}, false
	}
	dir := b.chooseRemoteDir(t)
	if dir == nil {
		return batch{}, false
	}
	out := b.createBatch(l, t, dir, host)
	return out, true
}

// Returns location for which batch should be created.
func (b *batchDispatcher) chooseLocation(host string) *LocationWorkload {
	for i := range b.workload {
		if b.workload[i].Size == 0 {
			continue
		}
		if slices.Contains(b.locationHosts[b.workload[i].Location], host) {
			return &b.workload[i]
		}
	}
	return nil
}

// Returns table for which batch should be created.
func (b *batchDispatcher) chooseTable(location *LocationWorkload) *TableWorkload {
	for i := range location.Tables {
		if location.Tables[i].Size == 0 {
			continue
		}
		return &location.Tables[i]
	}
	return nil
}

// Return remote dir for which batch should be created.
func (b *batchDispatcher) chooseRemoteDir(table *TableWorkload) *RemoteDirWorkload {
	for i := range table.RemoteDirs {
		if table.RemoteDirs[i].Size == 0 {
			continue
		}
		return &table.RemoteDirs[i]
	}
	return nil
}

// Returns batch and updates RemoteDirWorkload and its parents.
func (b *batchDispatcher) createBatch(l *LocationWorkload, t *TableWorkload, dir *RemoteDirWorkload, host string) batch {
	batchSize := b.batchSize * int(b.hostShardCnt[host])
	if batchSize <= 0 {
		panic("invalid batch size")
	}

	i := min(batchSize, len(dir.SSTables))
	sstables := dir.SSTables[:i]
	dir.SSTables = dir.SSTables[i:]

	var size int64
	for _, sst := range sstables {
		size += sst.Size
	}
	dir.Size -= size
	t.Size -= size
	l.Size -= size
	return batch{
		TableName:        dir.TableName,
		ManifestInfo:     dir.ManifestInfo,
		RemoteSSTableDir: dir.RemoteSSTableDir,
		Size:             size,
		SSTables:         sstables,
	}
}
