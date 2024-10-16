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
	workload              Workload
	batchSize             int
	expectedShardWorkload int64
	hostShardCnt          map[string]uint
	locationHosts         map[Location][]string
	hostToFailedDC        map[string][]string
}

func newBatchDispatcher(workload Workload, batchSize int, hostShardCnt map[string]uint, locationHosts map[Location][]string) *batchDispatcher {
	sortWorkload(workload)
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
		remainingBytes:        workload.TotalSize,
		workload:              workload,
		batchSize:             batchSize,
		expectedShardWorkload: workload.TotalSize / int64(shards),
		hostShardCnt:          hostShardCnt,
		locationHosts:         locationHosts,
		hostToFailedDC:        make(map[string][]string),
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

	for _, rdw := range bd.workload.RemoteDir {
		if rdw.Size != 0 || len(rdw.SSTables) != 0 {
			return errors.Errorf("expected all data to be restored, missing sstables from location %s table %s.%s: %v (%d bytes)",
				rdw.Location, rdw.Keyspace, rdw.Table, rdw.SSTables, rdw.Size)
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
	var rdw *RemoteDirWorkload
	for i, w := range bd.workload.RemoteDir {
		// Skip empty dir
		if w.Size == 0 {
			continue
		}
		// Skip dir from already failed dc
		if slices.Contains(bd.hostToFailedDC[host], w.DC) {
			continue
		}
		// Sip dir from location without access
		if !slices.Contains(bd.locationHosts[w.Location], host) {
			continue
		}
		rdw = &bd.workload.RemoteDir[i]
		break
	}
	if rdw == nil {
		return batch{}, false
	}
	return bd.createBatch(rdw, host)
}

// Returns batch and updates RemoteDirWorkload and its parents.
func (bd *batchDispatcher) createBatch(rdw *RemoteDirWorkload, host string) (batch, bool) {
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
				if i >= len(rdw.SSTables) {
					break
				}
				size += rdw.SSTables[i].Size
				i++
			}
			if i >= len(rdw.SSTables) {
				break
			}
			if size > sizeLimit {
				break
			}
		}
	} else {
		// Create batch containing node_shard_count*batch_size sstables.
		i = min(bd.batchSize*int(shardCnt), len(rdw.SSTables))
		for j := 0; j < i; j++ {
			size += rdw.SSTables[j].Size
		}
	}

	if i == 0 {
		return batch{}, false
	}
	// Extend batch if it was to leave less than
	// 1 sstable per shard for the next one.
	if len(rdw.SSTables)-i < int(shardCnt) {
		for ; i < len(rdw.SSTables); i++ {
			size += rdw.SSTables[i].Size
		}
	}

	sstables := rdw.SSTables[:i]
	rdw.SSTables = rdw.SSTables[i:]

	rdw.Size -= size
	bd.workload.TableSize[rdw.TableName] -= size
	bd.workload.LocationSize[rdw.Location] -= size
	bd.workload.TotalSize -= size
	return batch{
		TableName:        rdw.TableName,
		ManifestInfo:     rdw.ManifestInfo,
		RemoteSSTableDir: rdw.RemoteSSTableDir,
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
func (bd *batchDispatcher) ReportFailure(host string, b batch) error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	// Mark failed DC for host
	bd.hostToFailedDC[host] = append(bd.hostToFailedDC[host], b.DC)

	var rdw *RemoteDirWorkload
	for i := range bd.workload.RemoteDir {
		if bd.workload.RemoteDir[i].RemoteSSTableDir == b.RemoteSSTableDir {
			rdw = &bd.workload.RemoteDir[i]
		}
	}
	if rdw == nil {
		return errors.Errorf("unknown remote sstable dir %s", b.RemoteSSTableDir)
	}

	var newSST []RemoteSSTable
	newSST = append(newSST, b.SSTables...)
	newSST = append(newSST, rdw.SSTables...)

	rdw.SSTables = newSST
	rdw.Size += b.Size
	bd.workload.TableSize[b.TableName] += b.Size
	bd.workload.LocationSize[b.Location] += b.Size

	bd.wakeUpWaiting()
	return nil
}

func (bd *batchDispatcher) wakeUpWaiting() {
	close(bd.wait)
	bd.wait = make(chan struct{})
}

func sortWorkload(workload Workload) {
	// Order remote sstable dirs by table size, then by their size (decreasing).
	slices.SortFunc(workload.RemoteDir, func(a, b RemoteDirWorkload) int {
		ats := workload.TableSize[a.TableName]
		bts := workload.TableSize[b.TableName]
		if ats != bts {
			return int(bts - ats)
		}
		return int(b.Size - a.Size)
	})
	// Order sstables by their size (decreasing)
	for _, rdw := range workload.RemoteDir {
		slices.SortFunc(rdw.SSTables, func(a, b RemoteSSTable) int {
			return int(b.Size - a.Size)
		})
	}
}
