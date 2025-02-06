// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
)

// batchDispatcher is a tool for batching SSTables from
// Workload across different hosts during restore.
// It follows a few rules:
//
// - it dispatches batches from the RemoteDirWorkload with the biggest
// initial size first
//
// - it aims to optimize batch size according to batchSize param
//
// - it selects the biggest SSTables from RemoteDirWorkload first,
// so that batch contains SSTables of similar size (improved shard utilization)
//
// - it supports batch retry - failed batch can be re-tried by other
// hosts (see wait description for more information)
//
// - it supports host retry - host that failed to restore batch can still
// restore other batches (see hostFailedDC description for more information).
type batchDispatcher struct {
	// Guards all exported methods
	mu sync.Mutex
	// When there are no more batches to be restored,
	// but some already dispatched batches are still
	// being processed, idle hosts waits on wait chan.
	// They should wait, as in case currently processed
	// batch fails to be restored, they can be waked up
	// by batchDispatcher, and re-try to restore returned
	// batch on their own.
	wait chan struct{}

	// Const workload defined during indexing
	workload Workload
	// Mutable workloadProgress updated as batches are dispatched
	workloadProgress workloadProgress
	// For batchSize X, batches contain X*node_shard_cnt SSTables.
	// We always multiply batchSize by node_shard_cnt in order to
	// utilize all shards more equally.
	// For batchSize 0, batches contain N*node_shard_cnt SSTables
	// of total size up to 5% of node expected workload
	// (expectedShardWorkload*node_shard_cnt).
	batchSize int
	// Equals total_backup_size/($\sum_{node} shard_cnt(node)$)
	expectedShardWorkload int64
	// Stores host shard count
	hostShardCnt map[string]uint
}

func newBatchDispatcher(workload Workload, batchSize int, hostShardCnt map[string]uint, locationHosts map[backupspec.Location][]string) *batchDispatcher {
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
		workload:              workload,
		workloadProgress:      newWorkloadProgress(workload, locationHosts),
		batchSize:             batchSize,
		expectedShardWorkload: workload.TotalSize / int64(shards),
		hostShardCnt:          hostShardCnt,
	}
}

// Describes current state of SSTables that are yet to be batched.
type workloadProgress struct {
	// Bytes that are yet to be restored from given backed up DC.
	// They are decreased after a successful batch restoration.
	dcBytesToBeRestored map[string]int64
	// Marks which host failed to restore batches from which DCs.
	// When host failed to restore a batch from one backed up DC,
	// it can still restore other batches coming from different
	// DCs. This is a host re-try mechanism aiming to help with #3871.
	hostFailedDC map[string][]string
	// Stores which hosts have access to restore which DCs.
	// It assumes that the whole DC is backed up to a single
	// backup location.
	hostDCAccess map[string][]string
	// SSTables grouped by RemoteSSTableDir that are yet to
	// be batched. They are removed on batch dispatch, but can
	// be re-added when batch failed to be restored.
	// workloadProgress.remoteDir and Workload.RemoteDir have
	// corresponding indexes.
	remoteDir []remoteSSTableDirProgress
}

// Describes current state of SSTables from given RemoteSSTableDir
// that are yet to be batched.
type remoteSSTableDirProgress struct {
	RemainingSize     int64
	RemainingSSTables []RemoteSSTable
}

func newWorkloadProgress(workload Workload, locationHosts map[backupspec.Location][]string) workloadProgress {
	dcBytes := make(map[string]int64)
	locationDC := make(map[string][]string)
	p := make([]remoteSSTableDirProgress, len(workload.RemoteDir))
	for i, rdw := range workload.RemoteDir {
		dcBytes[rdw.DC] += rdw.Size
		locationDC[rdw.Location.StringWithoutDC()] = append(locationDC[rdw.Location.StringWithoutDC()], rdw.DC)
		p[i] = remoteSSTableDirProgress{
			RemainingSize:     rdw.Size,
			RemainingSSTables: rdw.SSTables,
		}
	}
	hostDCAccess := make(map[string][]string)
	for loc, hosts := range locationHosts {
		for _, h := range hosts {
			hostDCAccess[h] = append(hostDCAccess[h], locationDC[loc.StringWithoutDC()]...)
		}
	}
	return workloadProgress{
		dcBytesToBeRestored: dcBytes,
		hostFailedDC:        make(map[string][]string),
		hostDCAccess:        hostDCAccess,
		remoteDir:           p,
	}
}

// Checks if given host finished restoring all that it could.
func (wp workloadProgress) isDone(host string) bool {
	failed := wp.hostFailedDC[host]
	for _, dc := range wp.hostDCAccess[host] {
		// Host isn't done when there is still some data to be restored
		// from a DC that it has access to, and it didn't previously fail
		// to restore data from this DC.
		if !slices.Contains(failed, dc) && wp.dcBytesToBeRestored[dc] != 0 {
			return false
		}
	}
	return true
}

type batch struct {
	TableName
	*backupspec.ManifestInfo

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

// ValidateAllDispatched returns error if not all SSTables were dispatched.
func (bd *batchDispatcher) ValidateAllDispatched() error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	for i, rdp := range bd.workloadProgress.remoteDir {
		if rdp.RemainingSize != 0 || len(rdp.RemainingSSTables) != 0 {
			rdw := bd.workload.RemoteDir[i]
			return errors.Errorf("failed to restore sstables from location %s table %s.%s (%d bytes). See logs for more info",
				rdw.Location, rdw.Keyspace, rdw.Table, rdw.Size)
		}
	}
	for dc, bytes := range bd.workloadProgress.dcBytesToBeRestored {
		if bytes != 0 {
			return errors.Errorf("expected all data from DC %q to be restored (missing %d bytes): "+
				"internal progress calculation error", dc, bytes)
		}
	}
	return nil
}

// DispatchBatch returns batch to be restored or false when there is no more work to do.
// This method might hang and wait for sstables that might come from batches that
// failed to be restored (see batchDispatcher.wait description for more information).
// Because of that, it's important to call ReportSuccess or ReportFailure after
// each dispatched batch was attempted to be restored.
func (bd *batchDispatcher) DispatchBatch(ctx context.Context, host string) (batch, bool) {
	for {
		if ctx.Err() != nil {
			return batch{}, false
		}
		bd.mu.Lock()
		// Check if there is anything to do for this host
		if bd.workloadProgress.isDone(host) {
			bd.mu.Unlock()
			return batch{}, false
		}
		// Try to dispatch batch
		b, ok := bd.dispatchBatch(host)
		wait := bd.wait
		bd.mu.Unlock()
		if ok {
			return b, true
		}
		// Wait for SSTables that might return after failure
		select {
		case <-ctx.Done():
		case <-wait:
		}
	}
}

func (bd *batchDispatcher) dispatchBatch(host string) (batch, bool) {
	dirIdx := -1
	for i := range bd.workloadProgress.remoteDir {
		rdw := bd.workload.RemoteDir[i]
		// Skip empty dir
		if bd.workloadProgress.remoteDir[i].RemainingSize == 0 {
			continue
		}
		// Skip dir from already failed dc
		if slices.Contains(bd.workloadProgress.hostFailedDC[host], rdw.DC) {
			continue
		}
		// Sip dir from location without access
		if !slices.Contains(bd.workloadProgress.hostDCAccess[host], rdw.DC) {
			continue
		}
		dirIdx = i
		break
	}
	if dirIdx < 0 {
		return batch{}, false
	}
	return bd.createBatch(dirIdx, host)
}

// Returns batch from given RemoteSSTableDir and updates workloadProgress.
func (bd *batchDispatcher) createBatch(dirIdx int, host string) (batch, bool) {
	rdp := &bd.workloadProgress.remoteDir[dirIdx]
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
			for range shardCnt {
				if i >= len(rdp.RemainingSSTables) {
					break
				}
				size += rdp.RemainingSSTables[i].Size
				i++
			}
			if i >= len(rdp.RemainingSSTables) {
				break
			}
			if size > sizeLimit {
				break
			}
		}
	} else {
		// Create batch containing node_shard_count*batch_size sstables.
		i = min(bd.batchSize*int(shardCnt), len(rdp.RemainingSSTables))
		for j := range i {
			size += rdp.RemainingSSTables[j].Size
		}
	}

	if i == 0 {
		return batch{}, false
	}
	// Extend batch if it was to leave less than
	// 1 sstable per shard for the next one.
	if len(rdp.RemainingSSTables)-i < int(shardCnt) {
		for ; i < len(rdp.RemainingSSTables); i++ {
			size += rdp.RemainingSSTables[i].Size
		}
	}

	sstables := rdp.RemainingSSTables[:i]
	rdp.RemainingSSTables = rdp.RemainingSSTables[i:]
	rdw := bd.workload.RemoteDir[dirIdx]

	rdp.RemainingSize -= size
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

	dcBytes := bd.workloadProgress.dcBytesToBeRestored
	dcBytes[b.DC] -= b.Size
	// Mark batching as finished due to successful restore
	if dcBytes[b.DC] == 0 {
		bd.wakeUpWaiting()
	}
}

// ReportFailure notifies batchDispatcher that given batch failed to be restored.
func (bd *batchDispatcher) ReportFailure(host string, b batch) error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	// Mark failed DC for host
	bd.workloadProgress.hostFailedDC[host] = append(bd.workloadProgress.hostFailedDC[host], b.DC)

	dirIdx := -1
	for i := range bd.workload.RemoteDir {
		if bd.workload.RemoteDir[i].RemoteSSTableDir == b.RemoteSSTableDir {
			dirIdx = i
			break
		}
	}
	if dirIdx < 0 {
		return errors.Errorf("unknown remote sstable dir %s", b.RemoteSSTableDir)
	}

	rdp := &bd.workloadProgress.remoteDir[dirIdx]
	rdp.RemainingSSTables = append(b.SSTables, rdp.RemainingSSTables...)
	rdp.RemainingSize += b.Size

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
