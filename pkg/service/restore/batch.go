// Copyright (C) 2024 ScyllaDB

package restore

import (
	"slices"
	"sync"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

type batchDispatcher struct {
	mu   sync.Mutex
	wait chan struct{}

	remainingBytes        int64
	workload              []LocationWorkload
	batchSize             int
	expectedShardWorkload int64
	hostShardCnt          map[string]uint
	locationHosts         map[Location][]string
}

func newBatchDispatcher(workload []LocationWorkload, batchSize int, hostShardCnt map[string]uint, locationHosts map[Location][]string) *batchDispatcher {
	sortWorkloadBySizeDesc(workload)
	var size int64
	for _, t := range workload {
		size += t.Size
	}
	var shards uint
	for _, sh := range hostShardCnt {
		shards += sh
	}
	if shards == 0 {
		shards = 1
	}
	return &batchDispatcher{
		mu:                    sync.Mutex{},
		wait:                  make(chan struct{}),
		remainingBytes:        size,
		workload:              workload,
		batchSize:             batchSize,
		expectedShardWorkload: size / int64(shards),
		hostShardCnt:          hostShardCnt,
		locationHosts:         locationHosts,
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
func (bd *batchDispatcher) ValidateAllDispatched() error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	for _, lw := range bd.workload {
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
	if !bd.done() {
		return errors.Errorf("expected all data to be restored, internal progress calculation error")
	}
	return nil
}

// DispatchBatch returns batch restored or false when there is no more work to do.
// This method might hang and wait for sstables that might come from batches that
// failed to be restored. Because of that, it's important to call ReportSuccess
// or ReportFailure after each dispatched batch was attempted to be restored.
func (bd *batchDispatcher) DispatchBatch(host string) (batch, bool) {
	for {
		bd.mu.Lock()

		if bd.done() {
			bd.mu.Unlock()
			return batch{}, false
		}
		b, ok := bd.dispatchBatch(host)
		wait := bd.wait

		bd.mu.Unlock()

		if ok {
			return b, true
		}
		<-wait
	}
}

func (bd *batchDispatcher) done() bool {
	return bd.remainingBytes == 0
}

func (bd *batchDispatcher) dispatchBatch(host string) (batch, bool) {
	l := bd.chooseLocation(host)
	if l == nil {
		return batch{}, false
	}
	t := bd.chooseTable(l)
	if t == nil {
		return batch{}, false
	}
	dir := bd.chooseRemoteDir(t)
	if dir == nil {
		return batch{}, false
	}
	return bd.createBatch(l, t, dir, host)
}

// Returns location for which batch should be created.
func (bd *batchDispatcher) chooseLocation(host string) *LocationWorkload {
	for i := range bd.workload {
		if bd.workload[i].Size == 0 {
			continue
		}
		if slices.Contains(bd.locationHosts[bd.workload[i].Location], host) {
			return &bd.workload[i]
		}
	}
	return nil
}

// Returns table for which batch should be created.
func (bd *batchDispatcher) chooseTable(location *LocationWorkload) *TableWorkload {
	for i := range location.Tables {
		if location.Tables[i].Size == 0 {
			continue
		}
		return &location.Tables[i]
	}
	return nil
}

// Return remote dir for which batch should be created.
func (bd *batchDispatcher) chooseRemoteDir(table *TableWorkload) *RemoteDirWorkload {
	for i := range table.RemoteDirs {
		if table.RemoteDirs[i].Size == 0 {
			continue
		}
		return &table.RemoteDirs[i]
	}
	return nil
}

// Returns batch and updates RemoteDirWorkload and its parents.
func (bd *batchDispatcher) createBatch(l *LocationWorkload, t *TableWorkload, dir *RemoteDirWorkload, host string) (batch, bool) {
	shardCnt := bd.hostShardCnt[host]
	if shardCnt == 0 {
		shardCnt = 1
	}

	var i int
	var size int64
	if bd.batchSize == maxBatchSize {
		// Create batch containing multiple of node shard count sstables
		// and size up to 5% of expected node workload.
		expectedNodeWorkload := bd.expectedShardWorkload * int64(shardCnt)
		sizeLimit := expectedNodeWorkload / 20
		for {
			for j := 0; j < int(shardCnt); j++ {
				if i >= len(dir.SSTables) {
					break
				}
				size += dir.SSTables[i].Size
				i++
			}
			if i >= len(dir.SSTables) {
				break
			}
			if size > sizeLimit {
				break
			}
		}
	} else {
		// Create batch containing node_shard_count*batch_size sstables.
		i = min(bd.batchSize*int(shardCnt), len(dir.SSTables))
		for j := 0; j < i; j++ {
			size += dir.SSTables[j].Size
		}
	}

	if i == 0 {
		return batch{}, false
	}
	// Extend batch if it was to leave less than
	// 1 sstable per shard for the next one.
	if len(dir.SSTables)-i < int(shardCnt) {
		for ; i < len(dir.SSTables); i++ {
			size += dir.SSTables[i].Size
		}
	}

	sstables := dir.SSTables[:i]
	dir.SSTables = dir.SSTables[i:]

	dir.Size -= size
	t.Size -= size
	l.Size -= size
	return batch{
		TableName:        dir.TableName,
		ManifestInfo:     dir.ManifestInfo,
		RemoteSSTableDir: dir.RemoteSSTableDir,
		Size:             size,
		SSTables:         sstables,
	}, true
}

// ReportSuccess notifies batchDispatcher that given batch was restored successfully.
func (bd *batchDispatcher) ReportSuccess(b batch) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	bd.remainingBytes -= b.Size
	if bd.done() {
		bd.wakeUpWaiting()
	}
}

// ReportFailure notifies batchDispatcher that given batch failed to be restored.
func (bd *batchDispatcher) ReportFailure(b batch) error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	var (
		lw *LocationWorkload
		tw *TableWorkload
		rw *RemoteDirWorkload
	)
	for i := range bd.workload {
		if bd.workload[i].Location == b.Location {
			lw = &bd.workload[i]
		}
	}
	if lw == nil {
		return errors.Errorf("unknown location %s", b.Location)
	}
	for i := range lw.Tables {
		if lw.Tables[i].TableName == b.TableName {
			tw = &lw.Tables[i]
		}
	}
	if tw == nil {
		return errors.Errorf("unknown table %s", b.TableName)
	}
	for i := range tw.RemoteDirs {
		if tw.RemoteDirs[i].RemoteSSTableDir == b.RemoteSSTableDir {
			rw = &tw.RemoteDirs[i]
		}
	}
	if rw == nil {
		return errors.Errorf("unknown remote sstable dir %s", b.RemoteSSTableDir)
	}

	var newSST []RemoteSSTable
	newSST = append(newSST, b.SSTables...)
	newSST = append(newSST, rw.SSTables...)

	rw.SSTables = newSST
	rw.Size += b.Size
	tw.Size += b.Size
	lw.Size += b.Size

	bd.wakeUpWaiting()
	return nil
}

func (bd *batchDispatcher) wakeUpWaiting() {
	close(bd.wait)
	bd.wait = make(chan struct{})
}

func sortWorkloadBySizeDesc(workload []LocationWorkload) {
	slices.SortFunc(workload, func(a, b LocationWorkload) int {
		return int(b.Size - a.Size)
	})
	for _, loc := range workload {
		slices.SortFunc(loc.Tables, func(a, b TableWorkload) int {
			return int(b.Size - a.Size)
		})
		for _, tab := range loc.Tables {
			slices.SortFunc(tab.RemoteDirs, func(a, b RemoteDirWorkload) int {
				return int(b.Size - a.Size)
			})
			for _, dir := range tab.RemoteDirs {
				slices.SortFunc(dir.SSTables, func(a, b RemoteSSTable) int {
					return int(b.Size - a.Size)
				})
			}
		}
	}
}
