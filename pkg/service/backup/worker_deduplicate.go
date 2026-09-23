// Copyright (C) 2026 ScyllaDB

package backup

import (
	"bytes"
	"context"
	stdErr "errors"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

type deduplicateTestHooks interface {
	beforeDeduplicateHost()
	afterDeduplicateHost(skipped, uploaded, size int64)
}

// Deduplicate handles the deduplicate stage of the backup process.
// The implementation is expected to follow RFC document
// https://docs.google.com/document/d/1EtGlF6UGNy34D_7QsnCheaukp3UwVObZU56PBdd0CQ8/edit#heading=h.jl2qbpcarwp9
func (w *worker) Deduplicate(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	f := func(h hostInfo) error {
		w.Logger.Info(ctx, "Removing duplicated files from local snapshot", "host", h.IP)
		err := w.deduplicateHost(ctx, h)
		if err == nil {
			w.Logger.Info(ctx, "Done deduplication", "host", h.IP)
		}
		return err
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Removing duplicated files failed on host", "host", h.IP, "error", err)
	}

	return inParallelWithLimits(hosts, limits, f, notify)
}

func (w *worker) deduplicateHost(ctx context.Context, h hostInfo) error {
	if w.dth != nil {
		w.dth.beforeDeduplicateHost()
		defer func(sd []snapshotDir) {
			var skipped, uploaded, size int64
			for i := range sd {
				skipped += sd[i].SkippedBytesOffset
				uploaded += sd[i].Progress.Uploaded
				size += sd[i].Progress.Size
			}
			w.dth.afterDeduplicateHost(skipped, uploaded, size)
		}(w.hostSnapshotDirs(h))
	}

	dirs := w.hostSnapshotDirs(h)
	f := func(i int) (err error) {
		d := &dirs[i]
		dataDst := h.Location.RemotePath(w.remoteSSTableDir(h, *d))

		applyHolds := w.RetentionLockMode == RetentionLockEventBasedHold
		var holdHandler *eventBasedHoldHandler
		if applyHolds {
			// Initialize holdHandler
			apply := func(ctx context.Context, paths []string, hold bool) error {
				return w.holdAndWait(ctx, h.IP, dataDst, paths, hold, h.ID, d.Keyspace, d.Table)
			}
			holdHandler = newEventBasedHoldHandler(apply, eventBasedHoldBatchSize)
			// Feed local files
			for _, file := range d.Progress.files {
				holdHandler.addLocal(file.Name)
			}
			for _, name := range d.ScyllaManifests {
				holdHandler.addLocal(renameScyllaManifest(w.SnapshotTag, h.ID, name))
			}
			holdHandler.finalizeLocal()
		}

		remoteSSTableBundles := newSSTableBundlesByID()
		listOpts := &scyllaclient.RcloneListDirOpts{
			FilesOnly:          true,
			Recurse:            true,
			ShowEventBasedHold: applyHolds,
		}
		listErr := w.Client.RcloneListDirIter(ctx, h.IP, dataDst, listOpts, func(f *scyllaclient.RcloneListDirItem) {
			if applyHolds {
				holdHandler.addRemote(ctx, f.Name, f.EventBasedHold)
			}
			// Skip scylla manifests
			if strings.HasSuffix(f.Name, backupspec.ScyllaManifest) {
				return
			}
			// Skip versioned files, as deduplication is only interested
			// in the newest file versions (others have tag suffix which
			// makes them impossible to deduplicate).
			if _, version := SplitNameAndVersion(f.Name); version != "" {
				return
			}
			if err := remoteSSTableBundles.add(f.Name, f.Size); err != nil {
				w.Logger.Error(ctx, "Couldn't create remote sstable bundle info", "file", f.Name, "error", err)
			}
		})
		var finalizeErr error
		if applyHolds {
			finalizeErr = holdHandler.finalize(ctx)
		}
		if err := stdErr.Join(
			errors.Wrapf(listErr, "host %s: listing all files from %s", h.IP, dataDst),
			errors.Wrap(finalizeErr, "finalize event based holds"),
		); err != nil {
			return err
		}

		localSSTableBundles := newSSTableBundlesByID()
		for _, file := range d.Progress.files {
			if err := localSSTableBundles.add(file.Name, file.Size); err != nil {
				w.Logger.Error(ctx, "Couldn't create local sstable bundle info", "file", file.Name, "error", err)
			}
		}

		deduplicatedUUIDSSTables := w.deduplicateUUIDSStables(remoteSSTableBundles, localSSTableBundles)
		deduplicatedIntSSTables, versionedCnt, err := w.deduplicateIntSSTables(ctx, h.IP, dataDst, d.Path, remoteSSTableBundles, localSSTableBundles)
		if err != nil {
			return errors.Wrap(err, "deduplication based on .crc32 content")
		}
		if versionedCnt > 0 {
			// This metric is used to monitor edge case scenarios - populate it before returning
			// feature compatibility error and set it only when there is something to report.
			w.Metrics.SetVersionedFilesCount(w.ClusterID, h.ID, d.Keyspace, d.Table, versionedCnt)
		}
		if versionedCnt > 0 && w.RetentionLockMode != RetentionLockDisabled {
			// Creation of versioned sstable happens by copying old sstable version with appended version
			// suffix, then removing the original old version, then uploading the new version.
			// This is not compatible with event based hold backup, where default backup retention policy
			// is expected to prevent files from being deleted. We could make it work for object retention
			// lock backup, but since this is an edge-case that might not even exist with modern UUID based
			// SSTables, we simply don't need to support it. This would add both implementation and performance
			// implications, as it would require listing snapshot dirs in stage retention lock.
			return errors.Errorf("host %s: table %s.%s: snapshot contains sstables with integer based IDs which upload would result in creation of versioned files. "+
				"Upload of such snapshot is not compatible with --retention-lock-mode backup. "+
				"To proceed, make sure that new sstables use UUID based IDs and compact the integer based sstables away. "+
				"Then, start the backup task from scratch.", h.IP, d.Keyspace, d.Table)
		}

		d.willCreateVersioned = versionedCnt > 0
		deduplicated := make([]string, 0, len(deduplicatedUUIDSSTables)+len(deduplicatedIntSSTables))

		var totalSkipped int64
		for _, deduplicatedSet := range [][]fileInfo{deduplicatedIntSSTables, deduplicatedUUIDSSTables} {
			for _, fi := range deduplicatedSet {
				totalSkipped += fi.Size
				deduplicated = append(deduplicated, fi.Name)
			}
		}
		_, err = w.Client.RcloneDeletePathsInBatches(ctx, h.IP, d.Path, deduplicated, 1000)
		if err != nil {
			return errors.Wrap(err, "delete deduplicated files")
		}
		d.Progress.FilesSkippedCount += int64(len(deduplicated))
		d.SkippedBytesOffset += totalSkipped
		return nil
	}

	notify := func(i int, err error) {
		d := dirs[i]
		w.Logger.Error(ctx, "Failed to deduplicate host",
			"host", d.Host,
			"keyspace", d.Keyspace,
			"table", d.Table,
			"error", err,
		)
	}

	return parallel.Run(len(dirs), 1, f, notify)
}

func (w *worker) deduplicateUUIDSStables(remoteSSTables, localSSTables *sstableBundlesByID) []fileInfo {
	// SSTable bundle with UUID generation ID can be manually deduplicated
	// when SSTable bundle with the same UUID is already present on the remote.
	deduplicated := make([]fileInfo, 0)
	for id, localBundle := range localSSTables.uuidID {
		remoteBundle, ok := remoteSSTables.uuidID[id]
		if !ok {
			continue
		}
		if !isSSTableBundleSizeEqual(localBundle, remoteBundle) {
			continue
		}
		deduplicated = append(deduplicated, localBundle...)
	}
	return deduplicated
}

// versionedCnt is the sum of local sstable bundle files which couldn't
// be deduplicated even though they have counterpart remote bundle.
func (w *worker) deduplicateIntSSTables(ctx context.Context, host string, remoteDir, localDir string,
	remoteSSTables, localSSTables *sstableBundlesByID,
) (deduplicated []fileInfo, versionedCnt int, err error) {
	// Reference to SSTables 3.0 Data File Format
	// https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html

	// Per every SSTable files group, compare local <ID>-Digest.crc32 content
	// to the remote <ID>-Digest.crc32 content.
	// The same content implies that SSTable can be deduplicated and removed from local directory.
	for id, localBundle := range localSSTables.intID {
		remoteBundle, ok := remoteSSTables.intID[id]
		if !ok {
			continue
		}
		// At this point analyzed SSTable ID is present in both local and remote dirs.
		// Not being able to deduplicate it results in increasing versionedCnt.
		crc32Idx := slices.IndexFunc(localBundle, func(fi fileInfo) bool {
			return strings.HasSuffix(fi.Name, "Digest.crc32")
		})
		if crc32Idx == -1 {
			versionedCnt += len(localBundle)
			continue
		}
		crc32FileName := localBundle[crc32Idx].Name
		if !isSSTableBundleSizeEqual(localBundle, remoteBundle) {
			versionedCnt += len(localBundle)
			continue
		}

		remoteCRC32Path := path.Join(remoteDir, crc32FileName)
		remoteCRC32, err := w.Client.RcloneCat(ctx, host, remoteCRC32Path)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "get content of remote CRC32 %s", remoteCRC32Path)
		}

		localCRC32Path := path.Join(localDir, crc32FileName)
		localCRC32, err := w.Client.RcloneCat(ctx, host, localCRC32Path)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "get content of local CRC32 %s", localCRC32Path)
		}

		if bytes.Equal(localCRC32, remoteCRC32) {
			deduplicated = append(deduplicated, localBundle...)
		} else {
			versionedCnt += len(localBundle)
		}
	}
	return deduplicated, versionedCnt, nil
}

type sstableBundlesByID struct {
	intID  map[string][]fileInfo
	uuidID map[string][]fileInfo
}

func newSSTableBundlesByID() *sstableBundlesByID {
	return &sstableBundlesByID{
		intID:  make(map[string][]fileInfo),
		uuidID: make(map[string][]fileInfo),
	}
}

func (sst *sstableBundlesByID) add(name string, size int64) error {
	id, err := sstable.ExtractID(name)
	if err != nil {
		return errors.Wrap(err, "extract sstable generation id")
	}
	fi := fileInfo{
		Name: name,
		Size: size,
	}
	if isIntID(id) {
		sst.intID[id] = append(sst.intID[id], fi)
	} else {
		sst.uuidID[id] = append(sst.uuidID[id], fi)
	}
	return nil
}

func isIntID(id string) bool {
	_, err := strconv.Atoi(id)
	return err == nil
}

func isSSTableBundleSizeEqual(b1, b2 []fileInfo) bool {
	if len(b1) != len(b2) {
		return false
	}
	m := make(map[string]int64)
	for _, fi := range b1 {
		m[fi.Name] = fi.Size
	}
	for _, fi := range b2 {
		if size, ok := m[fi.Name]; !ok || size != fi.Size {
			return false
		}
	}
	return true
}

// catFunc returns the content of the file stored under remotePath.
// It abstracts scyllaclient.Client.RcloneCat bound to a given host.
type catFunc func(ctx context.Context, remotePath string) ([]byte, error)

// localSSTableBundle describes a single local sstable bundle
// (all components sharing the same generation ID) together with the
// summary of the remote files observed for this generation ID.
type localSSTableBundle struct {
	files  []fileInfo
	idType sstable.IDType
	// remoteCnt is the amount of observed remote files with this generation ID.
	remoteCnt int
	// matchedCnt is the amount of observed remote files with this generation ID
	// which have a local counterpart with the same name and size.
	matchedCnt int
}

// matchesRemote returns whether the observed remote bundle consists of exactly
// the same file names and sizes as the local one.
func (b *localSSTableBundle) matchesRemote() bool {
	return b.remoteCnt == len(b.files) && b.matchedCnt == b.remoteCnt
}

// sstableDeduplicator decides which local sstable files are already
// present in the backup location and can be skipped during upload.
//
// sstable.UUID sstables are deduplicated when there is a 1-1 mapping
// between local and remote component files with given UUID.
// Mapping is established based on file names and sizes.
//
// sstable.IntegerID sstables are deduplicated when there is a 1-1 mapping
// between local and remote component files with given integer ID.
// Mapping is established based on file names, sizes and contents of the sstable.ComponentDigestCRC.
//
// sstableDeduplicator returns files that can be deduplicated alongside
// the estimated amount of versioned files that will be created on upload.
// sstable.IntegerID sstable bundles are expected to create versioned files
// when remote components with the same integer ID exist, but there is no
// 1-1 mapping between them and the local ones.
// sstable.UUID sstables are never expected to create versioned files.
//
// Since the amount of remote sstables is expected to be greater than
// the amount of local ones, sstableDeduplicator first stores all the
// local sstables in memory (via addLocal) and performs deduplication checks
// on the fly as the remote sstables are processed (via addRemote) without
// storing them.
//
// Usage contract:
//   - feed all local files via addLocal
//   - call finalizeLocal
//   - feed all remote files via addRemote
//   - call result
type sstableDeduplicator struct {
	// cat reads the content of a single remote or local file.
	cat catFunc
	// remoteDir is the sstable dir in the backup location.
	remoteDir string
	// localDir is the snapshot dir on the host.
	localDir string
	// local maps sstable ID to its components.
	local map[string]*localSSTableBundle
	// localDone marks that finalizeLocal was called.
	localDone bool
}

func newSSTableDeduplicator(cat catFunc, remoteDir, localDir string) *sstableDeduplicator {
	return &sstableDeduplicator{
		cat:       cat,
		remoteDir: remoteDir,
		localDir:  localDir,
		local:     make(map[string]*localSSTableBundle),
	}
}

// addLocal records a local sstable component. It must be called before finalizeLocal.
func (d *sstableDeduplicator) addLocal(name string, size int64) error {
	if d.localDone {
		panic("cannot add local file after finalizeLocal")
	}
	// Skip scylla manifests
	if strings.HasSuffix(name, backupspec.ScyllaManifest) {
		return nil
	}
	id, err := sstable.ParseID(name)
	if err != nil {
		return errors.Wrap(err, "parse sstable generation id")
	}
	b, ok := d.local[id.ID]
	if !ok {
		b = &localSSTableBundle{idType: id.Type}
		d.local[id.ID] = b
	}
	b.files = append(b.files, fileInfo{
		Name: name,
		Size: size,
	})
	return nil
}

// finalizeLocal marks that all local files have already been added.
func (d *sstableDeduplicator) finalizeLocal() {
	d.localDone = true
}

// addRemote matches a single remote sstable component against the local bundles.
// It must be called after finalizeLocal.
func (d *sstableDeduplicator) addRemote(name string, size int64) error {
	if !d.localDone {
		panic("cannot add remote file before finalizeLocal")
	}
	// Skip scylla manifests
	if strings.HasSuffix(name, backupspec.ScyllaManifest) {
		return nil
	}
	// Skip versioned files, as deduplication is only interested
	// in the newest file versions (others have tag suffix which
	// makes them impossible to deduplicate).
	if _, version := SplitNameAndVersion(name); version != "" {
		return nil
	}
	id, err := sstable.ExtractID(name)
	if err != nil {
		return errors.Wrap(err, "extract sstable generation id")
	}
	b, ok := d.local[id]
	if !ok {
		// No local bundle - nothing to deduplicate
		return nil
	}
	b.remoteCnt++
	if slices.ContainsFunc(b.files, func(fi fileInfo) bool {
		return fi.Name == name && fi.Size == size
	}) {
		b.matchedCnt++
	}
	return nil
}

// result returns the files which can be deduplicated alongside
// the amount of files which are expected to create versioned files on upload.
// It must be called after all remote files have been added.
func (d *sstableDeduplicator) result(ctx context.Context) (deduplicated []fileInfo, versionedCnt int, err error) {
	for _, b := range d.local {
		if b.remoteCnt == 0 {
			// No remote counterpart - nothing to deduplicate
			continue
		}

		// Deduplicate sstable.UUID based on file names and sizes
		if b.idType == sstable.UUID {
			if b.matchesRemote() {
				deduplicated = append(deduplicated, b.files...)
			}
			continue
		}

		// Deduplicate sstable.IntegerID based on file names, sizes
		// and sstable.ComponentDigestCRC contents.
		if !b.matchesRemote() {
			// Uploading a bundle which has a remote counterpart with
			// a different set of components results in versioned files.
			versionedCnt += len(b.files)
			continue
		}

		equal, err := d.equalDigest(ctx, b)
		if err != nil {
			return nil, 0, err
		}
		if equal {
			deduplicated = append(deduplicated, b.files...)
		} else {
			versionedCnt += len(b.files)
		}
	}
	return deduplicated, versionedCnt, nil
}

// equalDigest compares the content of the local and remote
// sstable.ComponentDigestCRC of the given bundle.
// Bundles with no such component are never equal.
func (d *sstableDeduplicator) equalDigest(ctx context.Context, b *localSSTableBundle) (bool, error) {
	idx := slices.IndexFunc(b.files, func(fi fileInfo) bool {
		return strings.HasSuffix(fi.Name, string(sstable.ComponentDigestCRC))
	})
	if idx == -1 {
		return false, nil
	}
	name := b.files[idx].Name

	remotePath := path.Join(d.remoteDir, name)
	remoteDigest, err := d.cat(ctx, remotePath)
	if err != nil {
		return false, errors.Wrapf(err, "get content of remote CRC32 %s", remotePath)
	}

	localPath := path.Join(d.localDir, name)
	localDigest, err := d.cat(ctx, localPath)
	if err != nil {
		return false, errors.Wrapf(err, "get content of local CRC32 %s", localPath)
	}

	return bytes.Equal(localDigest, remoteDigest), nil
}
