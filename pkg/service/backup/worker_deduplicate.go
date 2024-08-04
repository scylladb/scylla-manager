// Copyright (C) 2024 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
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
			for _, v := range sd {
				skipped += v.Progress.Skipped
				uploaded += v.Progress.Uploaded
				size += v.Progress.Size
			}
			w.dth.afterDeduplicateHost(skipped, uploaded, size)
		}(w.hostSnapshotDirs(h))
	}

	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "set rate limit")
	}

	dirs := w.hostSnapshotDirs(h)
	f := func(i int) (err error) {
		d := dirs[i]
		dataDst := h.Location.RemotePath(w.remoteSSTableDir(h, d))

		remoteSSTableBundles := newSSTableBundlesByID()
		listOpts := &scyllaclient.RcloneListDirOpts{
			FilesOnly: true,
			Recurse:   true,
		}
		if err := w.Client.RcloneListDirIter(ctx, h.IP, dataDst, listOpts, func(f *scyllaclient.RcloneListDirItem) {
			if err := remoteSSTableBundles.add(f.Name, f.Size); err != nil {
				w.Logger.Error(ctx, "Couldn't create remote sstable bundle info", "file", f.Name, "error", err)
			}
		}); err != nil {
			return errors.Wrapf(err, "host %s: listing all files from %s", h.IP, dataDst)
		}

		localSSTableBundles := newSSTableBundlesByID()
		for _, file := range d.Progress.files {
			if err := localSSTableBundles.add(file.Name, file.Size); err != nil {
				w.Logger.Error(ctx, "Couldn't create local sstable bundle info", "file", file.Name, "error", err)
			}
		}

		deduplicatedByUUID, err := w.deduplicateUUIDSStables(ctx, d.Path, h.IP, remoteSSTableBundles, localSSTableBundles)
		if err != nil {
			return errors.Wrap(err, "deduplicate based on UUID as generation id content")
		}
		d.Progress.Skipped += deduplicatedByUUID

		deduplicatedByCrc32, err := w.deduplicateIntSSTables(ctx, h.IP, dataDst, d.Path, remoteSSTableBundles, localSSTableBundles)
		if err != nil {
			return errors.Wrap(err, "deduplication based on .crc32 content")
		}
		d.Progress.Skipped += deduplicatedByCrc32

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

func (w *worker) deduplicateUUIDSStables(ctx context.Context, host string, snapshotDir string,
	remoteSSTables, localSSTables *sstableBundlesByID,
) (deduplicated int64, err error) {
	// SSTable bundle with UUID generation ID can be manually deduplicated
	// when SSTable bundle with the same UUID is already present on the remote.
	for id, localBundle := range localSSTables.uuidID {
		remoteBundle, ok := remoteSSTables.uuidID[id]
		if !ok {
			continue
		}
		if !isSSTableBundleSizeEqual(localBundle, remoteBundle) {
			continue
		}
		// Remove duplicated SSTable from local snapshot
		for _, fi := range localBundle {
			localPath := path.Join(snapshotDir, fi.Name)
			w.Logger.Debug(ctx, "Removing local snapshot file (deduplication based on generation UUID)",
				"host", host, "file", localPath)
			if err := w.Client.RcloneDeleteFile(ctx, host, localPath); err != nil {
				return deduplicated, errors.Wrapf(err, "delete local snapshot's SSTable file %s", localPath)
			}
			deduplicated += fi.Size
		}
	}

	return deduplicated, nil
}

func (w *worker) deduplicateIntSSTables(ctx context.Context, host string, remoteDir, localDir string,
	remoteSSTables, localSSTables *sstableBundlesByID,
) (deduplicated int64, err error) {
	// Reference to SSTables 3.0 Data File Format
	// https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html

	// Per every SSTable files group, compare local <ID>-Digest.crc32 content
	// to the remote <ID>-Digest.crc32 content.
	// The same content implies that SSTable can be deduplicated and removed from local directory.
	for id, localBundle := range localSSTables.intID {
		crc32Idx := slices.IndexFunc(localBundle, func(fi fileInfo) bool {
			return strings.HasSuffix(fi.Name, "Digest.crc32")
		})
		if crc32Idx == -1 {
			continue
		}
		crc32FileName := localBundle[crc32Idx].Name
		remoteBundle, ok := remoteSSTables.intID[id]
		if !ok {
			continue
		}
		if !isSSTableBundleSizeEqual(localBundle, remoteBundle) {
			continue
		}

		remoteCRC32Path := path.Join(remoteDir, crc32FileName)
		remoteCRC32, err := w.Client.RcloneCat(ctx, host, remoteCRC32Path)
		if err != nil {
			return deduplicated, errors.Wrapf(err, "get content of remote CRC32 %s", remoteCRC32Path)
		}

		localCRC32Path := path.Join(localDir, crc32FileName)
		localCRC32, err := w.Client.RcloneCat(ctx, host, localCRC32Path)
		if err != nil {
			return deduplicated, errors.Wrapf(err, "get content of local CRC32 %s", localCRC32Path)
		}

		if !bytes.Equal(localCRC32, remoteCRC32) {
			continue
		}
		for _, fi := range localBundle {
			localPath := path.Join(localDir, fi.Name)
			w.Logger.Debug(ctx, "Removing local snapshot file (deduplication based on .crc32)", "host", host, "file", localPath)
			if err := w.Client.RcloneDeleteFile(ctx, host, localPath); err != nil {
				return deduplicated, errors.Wrapf(err, "delete local snapshot's SSTable file %s", localPath)
			}
			deduplicated += fi.Size
		}
	}
	return deduplicated, nil
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
