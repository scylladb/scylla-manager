// Copyright (C) 2024 ScyllaDB

package restore

import (
	"slices"
	"sync"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type batchDispatcher struct {
	mu sync.Mutex

	expectedShardWorkload int64
	hostShardCnt          map[string]uint
	// In the context of a single node, only one worker can be restoring
	// given table as streaming downloaded data requires exclusive access
	// to table's upload dir.
	lockedWorkers map[TableHost]uint
	workload      []TableWorkload
}

// TableHost represents combination of table and host.
type TableHost struct {
	TableName
	Host string
}

func newTableHost(keyspace, table, host string) TableHost {
	return TableHost{
		TableName: TableName{
			Keyspace: keyspace,
			Table:    table,
		},
		Host: host,
	}
}

func newBatchDispatcher(workload []TableWorkload, hostToShardCnt map[string]uint) *batchDispatcher {
	sortDecBySize(workload)
	var size int64
	for _, t := range workload {
		size += t.Size
	}
	var shards uint
	for _, sh := range hostToShardCnt {
		shards += sh
	}
	return &batchDispatcher{
		mu:                    sync.Mutex{},
		expectedShardWorkload: size / int64(shards),
		hostShardCnt:          hostToShardCnt,
		lockedWorkers:         make(map[TableHost]uint),
		workload:              workload,
	}
}

type batch struct {
	TableName
	Location     Location
	ClusterID    uuid.UUID
	DC           string
	NodeID       string
	TableVersion string

	RemoteSSTableDir string
	Size             int64
	SSTables         []RemoteSSTable
}

func (b batch) Files() []string {
	var files []string
	for _, sst := range b.SSTables {
		files = append(files, sst.Files...)
	}
	return files
}

func (b batch) VersionedFiles() []string {
	var files []string
	for _, sst := range b.SSTables {
		if sst.Versioned {
			files = append(files, sst.Files...)
		}
	}
	return files
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

func (b batch) UploadDir() string {
	return UploadTableDir(b.Keyspace, b.Table, b.TableVersion)
}

// DispatchBatch batch to be restored or false when there is no more work to do.
func (b *batchDispatcher) DispatchBatch(host string, workerID uint) (batch, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	t := b.chooseTable(host, workerID)
	if t == nil {
		return batch{}, false
	}
	dir := b.chooseRemoteDir(t)
	if dir == nil {
		return batch{}, false
	}
	out := b.createBatch(t, dir, host)
	return out, true
}

// Returns table for which batch should be created:
// - respects per host table locking.
// - prefers tables of bigger initial size.
func (b *batchDispatcher) chooseTable(host string, workerID uint) *TableWorkload {
	for idx, t := range b.workload {
		// Skip tables locked by other workers from the same host
		th := newTableHost(t.Keyspace, t.Table, host)
		if id, ok := b.lockedWorkers[th]; ok {
			if id != workerID {
				continue
			}
		} else {
			b.lockedWorkers[th] = workerID
		}
		// Skip already restored tables
		if t.Size == 0 {
			continue
		}
		return &b.workload[idx]
	}
	return nil
}

// Return remote dir for which batch should be created.
// - prefers dirs of bigger current size.
func (b *batchDispatcher) chooseRemoteDir(table *TableWorkload) *RemoteDirWorkload {
	chosenIdx := -1
	for idx := range table.RemoteDirs {
		// Skip already restored remote dirs
		if table.RemoteDirs[idx].Size == 0 {
			continue
		}
		if chosenIdx == -1 || table.RemoteDirs[chosenIdx].Size < table.RemoteDirs[idx].Size {
			chosenIdx = idx
		}
	}
	if chosenIdx == -1 {
		panic("no dir to restore")
	}
	return &table.RemoteDirs[chosenIdx]
}

// Returns batch and updates RemoteDirWorkload.
func (b *batchDispatcher) createBatch(t *TableWorkload, dir *RemoteDirWorkload, host string) batch {
	shardCnt, ok := b.hostShardCnt[host]
	if !ok {
		panic("no shard cnt for host: " + host)
	}

	var (
		idx                  int
		size                 int64
		expectedNodeWorkload = b.expectedShardWorkload * int64(shardCnt)
	)
	for {
		for j := 0; j < int(shardCnt); j++ {
			if idx >= len(dir.SSTables) {
				break
			}
			size += dir.SSTables[idx].Size
			idx++
		}
		if idx >= len(dir.SSTables) {
			break
		}
		// Limit batch size to 5% of total expected node workload
		if size > expectedNodeWorkload/20 {
			break
		}
	}
	if idx == 0 {
		panic("no sstables for batch")
	}

	sstables := dir.SSTables[:idx]
	dir.SSTables = dir.SSTables[idx:]
	dir.Size -= size
	t.Size -= size
	return batch{
		TableName:        dir.TableName,
		Location:         dir.Location,
		ClusterID:        dir.ClusterID,
		DC:               dir.DC,
		NodeID:           dir.NodeID,
		TableVersion:     dir.TableVersion,
		RemoteSSTableDir: dir.RemoteSSTableDir,
		Size:             size,
		SSTables:         sstables,
	}
}

func sortDecBySize(tables []TableWorkload) {
	cmp := func(a, b int64) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		default:
			return 0
		}
	}

	slices.SortFunc(tables, func(a, b TableWorkload) int {
		return cmp(a.Size, b.Size)
	})

	for _, t := range tables {
		slices.SortFunc(t.RemoteDirs, func(a, b RemoteDirWorkload) int {
			return cmp(a.Size, b.Size)
		})

		for idx := range t.RemoteDirs {
			slices.SortFunc(t.RemoteDirs[idx].SSTables, func(a, b RemoteSSTable) int {
				return cmp(a.Size, b.Size)
			})
		}
	}
}
