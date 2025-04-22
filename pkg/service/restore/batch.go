// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
)

// batchDispatcher is a tool for batching SSTables from
// Workload across different hosts during restore.
// It follows a few rules:
//
// - all SSTables within a batch have the same batchType
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

func newBatchDispatcher(workload Workload, batchSize int, hostShardCnt map[string]uint, locationInfo []LocationInfo) *batchDispatcher {
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
		workloadProgress:      newWorkloadProgress(workload, locationInfo),
		batchSize:             batchSize,
		expectedShardWorkload: workload.TotalSize / int64(shards),
		hostShardCnt:          hostShardCnt,
	}
}

// Describes current state of SSTables that are yet to be batched.
type workloadProgress struct {
	// SSTables that are yet to be restored from given backed up DC.
	// They are decreased after a successful batch restoration.
	dcSSTableToBeRestored map[string]int
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
	RemainingSSTables map[batchType][]RemoteSSTable
}

func (dp *remoteSSTableDirProgress) RemainingSSTableCnt() int {
	out := 0
	for _, ssts := range dp.RemainingSSTables {
		out += len(ssts)
	}
	return out
}

func newWorkloadProgress(workload Workload, locationInfo []LocationInfo) workloadProgress {
	dcSSTable := make(map[string]int)
	p := make([]remoteSSTableDirProgress, len(workload.RemoteDir))
	for i, rdw := range workload.RemoteDir {
		dcSSTable[rdw.DC] += len(rdw.SSTables)
		p[i] = remoteSSTableDirProgress{
			RemainingSSTables: groupSSTablesByBatchType(rdw.SSTables),
		}
	}
	return workloadProgress{
		dcSSTableToBeRestored: dcSSTable,
		hostFailedDC:          make(map[string][]string),
		hostDCAccess:          getHostDCAccess(locationInfo),
		remoteDir:             p,
	}
}

func getHostDCAccess(locationInfo []LocationInfo) map[string][]string {
	hostDCAccess := map[string][]string{}
	for _, l := range locationInfo {
		for dc, hosts := range l.DCHosts {
			for _, h := range hosts {
				hostDCAccess[h] = append(hostDCAccess[h], dc)
			}
		}
	}
	return hostDCAccess
}

// Checks if given host finished restoring all that it could.
func (wp workloadProgress) isDone(host string) bool {
	failed := wp.hostFailedDC[host]
	for _, dc := range wp.hostDCAccess[host] {
		// Host isn't done when there are still some sstables to be restored
		// from a DC that it has access to, and it didn't previously fail
		// to restore data from this DC.
		if !slices.Contains(failed, dc) && wp.dcSSTableToBeRestored[dc] > 0 {
			return false
		}
	}
	return true
}

type batch struct {
	TableName
	*ManifestInfo

	batchType        batchType
	RemoteSSTableDir string
	Size             int64
	SSTables         []RemoteSSTable
}

// Dividing batches by simplifies the restore procedure:
// - Files from versioned batches need to be downloaded one by one
// in order to rename them on the fly with Rclone API.
// - Batches with sstable.UUID type can be restored with native Scylla restore API.
type batchType struct {
	// All SSTables within a batch have the same ID type
	IDType sstable.IDType
	// All SSTables within a batch are either versioned or not.
	Versioned bool
	// In theory, batchType{IDType: sstable.UUID, Versioned: true} shouldn't exist
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
		ids = append(ids, sst.ID.ID)
	}
	return ids
}

// TOC returns a list of batch's sstable.ComponentTOC.
func (b batch) TOC() []string {
	out := make([]string, 0, len(b.SSTables))
	for _, sst := range b.SSTables {
		out = append(out, sst.TOC)
	}
	return out
}

// ValidateAllDispatched returns error if not all SSTables were dispatched.
func (bd *batchDispatcher) ValidateAllDispatched() error {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	for i, rdp := range bd.workloadProgress.remoteDir {
		if failed := rdp.RemainingSSTableCnt(); failed > 0 {
			rdw := bd.workload.RemoteDir[i]
			return errors.Errorf("failed to restore %d sstables of %s.%s from location %s. See logs for more info",
				failed, rdw.Keyspace, rdw.Table, rdw.Location)
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
		if bd.workloadProgress.remoteDir[i].RemainingSSTableCnt() == 0 {
			continue
		}
		// Skip dir from already failed dc
		if slices.Contains(bd.workloadProgress.hostFailedDC[host], rdw.DC) {
			continue
		}
		// Skip dir from location without access
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

	// Choose batch type and candidate sstables
	var batchT batchType
	var sstables []RemoteSSTable
	for bt, ssts := range rdp.RemainingSSTables {
		if len(ssts) > 0 {
			batchT = bt
			sstables = ssts
			break
		}
	}
	if len(sstables) == 0 {
		return batch{}, false
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
				if i >= len(sstables) {
					break
				}
				size += sstables[i].Size
				i++
			}
			if i >= len(sstables) {
				break
			}
			if size > sizeLimit {
				break
			}
		}
	} else {
		// Create batch containing node_shard_count*batch_size sstables.
		i = min(bd.batchSize*int(shardCnt), len(sstables))
		for j := range i {
			size += sstables[j].Size
		}
	}

	if i == 0 {
		return batch{}, false
	}
	// Extend batch if it was to leave less than
	// 1 sstable per shard for the next one.
	if len(sstables)-i < int(shardCnt) {
		for ; i < len(sstables); i++ {
			size += sstables[i].Size
		}
	}

	rdp.RemainingSSTables[batchT] = sstables[i:]
	rdw := bd.workload.RemoteDir[dirIdx]

	return batch{
		TableName:        rdw.TableName,
		ManifestInfo:     rdw.ManifestInfo,
		batchType:        batchT,
		RemoteSSTableDir: rdw.RemoteSSTableDir,
		Size:             size,
		SSTables:         sstables[:i],
	}, true
}

// ReportSuccess notifies batchDispatcher that given batch was restored successfully.
func (bd *batchDispatcher) ReportSuccess(b batch) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	dcSSTables := bd.workloadProgress.dcSSTableToBeRestored
	dcSSTables[b.DC] -= len(b.SSTables)
	// Mark batching as finished due to successful restore
	if dcSSTables[b.DC] == 0 {
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
	rdp.RemainingSSTables[b.batchType] = append(rdp.RemainingSSTables[b.batchType], b.SSTables...)

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

func groupSSTablesByBatchType(sstables []RemoteSSTable) map[batchType][]RemoteSSTable {
	out := make(map[batchType][]RemoteSSTable)
	for _, sst := range sstables {
		bt := batchType{
			IDType:    sst.ID.Type,
			Versioned: sst.Versioned,
		}
		out[bt] = append(out[bt], sst)
	}
	return out
}
