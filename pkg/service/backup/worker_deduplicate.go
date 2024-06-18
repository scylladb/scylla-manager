// Copyright (C) 2024 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"path"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

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
	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "set rate limit")
	}

	dirs := w.hostSnapshotDirs(h)
	f := func(i int) (err error) {
		d := dirs[i]
		dataDst := h.Location.RemotePath(w.remoteSSTableDir(h, d))

		// Reference to SSTables 3.0 Data File Format
		// https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html

		// Iterate over all SSTable IDs and group files per ID.
		ssTablesGroupByID := make(map[string][]string)
		for _, file := range d.Progress.files {
			id := sstable.ExtractID(file.Name)
			ssTablesGroupByID[id] = append(ssTablesGroupByID[id], file.Name)
		}

		// Per every SSTable files group, compare local <ID>-Digest.crc32 content
		// to the remote <ID>-Digest.crc32 content.
		// The same content implies that SSTable can be deduplicated and removed from local directory.
		for _, ssTableContent := range ssTablesGroupByID {
			crc32Idx := slices.IndexFunc(ssTableContent, func(s string) bool {
				return strings.HasSuffix(s, "Digest.crc32")
			})
			if crc32Idx == -1 {
				continue
			}

			remoteCRC32FileNameWithPath := path.Join(dataDst, ssTableContent[crc32Idx])
			remoteCRC32, err := w.Client.RcloneCat(ctx, h.IP, remoteCRC32FileNameWithPath)
			if err != nil {
				if strings.Contains(err.Error(), "object not found") {
					continue
				}
				return errors.Wrapf(err, "cannot get content of remote CRC32 %s", remoteCRC32FileNameWithPath)
			}

			localCRC32FileNameWithPath := path.Join(d.Path, ssTableContent[crc32Idx])
			localCRC32, err := w.Client.RcloneCat(ctx, h.IP, localCRC32FileNameWithPath)
			if err != nil {
				return errors.Wrapf(err, "cannot get content of local CRC32 %s", localCRC32FileNameWithPath)
			}

			// If checksums are equal, then it means that SSTable can be deduplicated as there is no need in
			// transferring it again to the remote storage.
			// Deduplication here means to remove SSTable files from local storage.
			if bytes.Equal(localCRC32, remoteCRC32) {
				for _, fileToBeRemoved := range ssTableContent {
					localSSTableFileNameWithPath := path.Join(d.Path, fileToBeRemoved)
					w.Logger.Info(ctx, "Removing local snapshot file (deduplication)", "host", h.IP, "file", localSSTableFileNameWithPath)
					if err := w.Client.RcloneDeleteFile(ctx, h.IP, localSSTableFileNameWithPath); err != nil {
						return errors.Wrapf(err, "cannot delete local snapshot's SSTable file %s", localSSTableFileNameWithPath)
					}
				}
			}
		}

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
