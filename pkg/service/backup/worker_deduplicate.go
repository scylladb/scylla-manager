// Copyright (C) 2024 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"fmt"
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

		remoteFilesSSTableIDSet := make(map[string]struct{})
		listOpts := &scyllaclient.RcloneListDirOpts{
			FilesOnly: true,
			Recurse:   true,
		}
		if err := w.Client.RcloneListDirIter(ctx, h.IP, dataDst, listOpts, func(f *scyllaclient.RcloneListDirItem) {
			id, err := sstable.ExtractID(f.Name)
			if err != nil {
				// just log and continue (should never happen)
				w.Logger.Error(ctx, "Extracting SSTable generation ID of remote SSTable", "error", err)
				return
			}
			remoteFilesSSTableIDSet[id] = struct{}{}
		}); err != nil {
			return errors.Wrapf(err, "host %s: listing all files from %s", h.IP, dataDst)
		}

		// Iterate over all SSTable IDs and group files per ID.
		ssTablesGroupByID := make(map[string][]string)
		for _, file := range d.Progress.files {
			id, err := sstable.ExtractID(file.Name)
			if err != nil {
				// just log and continue
				w.Logger.Error(ctx, "Extracting SSTable generation ID", "error", err)
				continue
			}
			ssTablesGroupByID[id] = append(ssTablesGroupByID[id], file.Name)
		}

		deduplicatedByUUID, err := w.basedOnUUIDGenerationAvailability(ctx, d, h, remoteFilesSSTableIDSet, ssTablesGroupByID)
		if err != nil {
			return errors.Wrap(err, "deduplication based on UUID as generation id content")
		}
		d.Progress.Skipped += deduplicatedByUUID

		deduplicatedByCrc32, err := w.basedOnCrc32Content(ctx, d, h, dataDst, remoteFilesSSTableIDSet, ssTablesGroupByID)
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

func (w *worker) basedOnUUIDGenerationAvailability(ctx context.Context, d snapshotDir, h hostInfo,
	remoteSSTables map[string]struct{}, ssTablesGroupByID map[string][]string,
) (deduplicated int64, err error) {
	// Per every SSTable files group, check if the name uses UUID to identify the SSTable generation.
	// If the above is true, then c4heck if files exists in the remote storage and remove it locally if exists in rmeote.
	for id, ssTableContent := range ssTablesGroupByID {
		// Check if id is an integer (no UUID)
		if _, err := strconv.Atoi(id); err == nil {
			continue
		}
		// Check if SSTable is available on remote already
		if _, ok := remoteSSTables[id]; !ok {
			continue
		}
		// Remove duplicated SSTable from local snapshot
		for _, file := range ssTableContent {
			// remove if exists in remote
			localSSTableFileNameWithPath := path.Join(d.Path, file)
			w.Logger.Debug(ctx, "Removing local snapshot file (deduplication based on generation UUID)",
				"host", h.IP, "file", localSSTableFileNameWithPath)
			if err := w.Client.RcloneDeleteFile(ctx, h.IP, localSSTableFileNameWithPath); err != nil {
				return deduplicated, errors.Wrapf(err, "cannot delete local snapshot's SSTable file %s", localSSTableFileNameWithPath)
			}
			idx := slices.IndexFunc(d.Progress.files, func(info fileInfo) bool {
				return info.Name == file
			})
			if idx == -1 {
				return deduplicated, fmt.Errorf("no file %s in indexed files", file)
			}
			deduplicated += d.Progress.files[idx].Size
		}
	}

	return deduplicated, nil
}

func (w *worker) basedOnCrc32Content(ctx context.Context, d snapshotDir, h hostInfo, dataDst string,
	remoteSSTables map[string]struct{}, ssTablesGroupByID map[string][]string,
) (deduplicated int64, err error) {
	// Reference to SSTables 3.0 Data File Format
	// https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html

	// Per every SSTable files group, compare local <ID>-Digest.crc32 content
	// to the remote <ID>-Digest.crc32 content.
	// The same content implies that SSTable can be deduplicated and removed from local directory.
	for id, ssTableContent := range ssTablesGroupByID {
		crc32Idx := slices.IndexFunc(ssTableContent, func(s string) bool {
			return strings.HasSuffix(s, "Digest.crc32")
		})
		if crc32Idx == -1 {
			continue
		}
		if _, ok := remoteSSTables[id]; !ok {
			continue
		}

		remoteCRC32FileNameWithPath := path.Join(dataDst, ssTableContent[crc32Idx])
		remoteCRC32, err := w.Client.RcloneCat(ctx, h.IP, remoteCRC32FileNameWithPath)
		if err != nil {
			if scyllaclient.StatusCodeOf(err) == 404 {
				continue
			}
			return deduplicated, errors.Wrapf(err, "cannot get content of remote CRC32 %s", remoteCRC32FileNameWithPath)
		}

		localCRC32FileNameWithPath := path.Join(d.Path, ssTableContent[crc32Idx])
		localCRC32, err := w.Client.RcloneCat(ctx, h.IP, localCRC32FileNameWithPath)
		if err != nil {
			return deduplicated, errors.Wrapf(err, "cannot get content of local CRC32 %s", localCRC32FileNameWithPath)
		}

		// If checksums are equal, then it means that SSTable can be deduplicated as there is no need in
		// transferring it again to the remote storage.
		// Deduplication here means to remove SSTable files from local storage.
		if bytes.Equal(localCRC32, remoteCRC32) {
			// validate sizes first
			for _, file := range ssTableContent {
				remoteFilePath := path.Join(dataDst, file)
				remoteInfo, err := w.Client.RcloneFileInfo(ctx, h.IP, remoteFilePath)
				if err != nil {
					return deduplicated, errors.Wrapf(err, "get file info %s", remoteFilePath)
				}
				idx := slices.IndexFunc(d.Progress.files, func(info fileInfo) bool {
					return info.Name == file
				})
				if idx == -1 {
					return deduplicated, fmt.Errorf("no file %s in indexed files", file)
				}
				if remoteInfo.Size != d.Progress.files[idx].Size {
					w.Logger.Debug(ctx, "Equal CRC32 hash but different sizes, skipping", "remote", remoteCRC32FileNameWithPath)
				}
			}
			for _, fileToBeRemoved := range ssTableContent {
				localSSTableFileNameWithPath := path.Join(d.Path, fileToBeRemoved)
				w.Logger.Debug(ctx, "Removing local snapshot file (deduplication based on .crc32)", "host", h.IP, "file", localSSTableFileNameWithPath)
				if err := w.Client.RcloneDeleteFile(ctx, h.IP, localSSTableFileNameWithPath); err != nil {
					return deduplicated, errors.Wrapf(err, "cannot delete local snapshot's SSTable file %s", localSSTableFileNameWithPath)
				}
				idx := slices.IndexFunc(d.Progress.files, func(info fileInfo) bool {
					return info.Name == fileToBeRemoved
				})
				if idx == -1 {
					return deduplicated, fmt.Errorf("no file %s in indexed files", fileToBeRemoved)
				}
				deduplicated += d.Progress.files[idx].Size
			}
		}
	}
	return deduplicated, nil
}
