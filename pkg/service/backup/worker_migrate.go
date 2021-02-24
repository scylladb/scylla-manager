// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

// MigrateManifests migrates V1 manifests to V2 format.
// V1 manifests are not deleted since we want to keep backups read only.
func (w *worker) MigrateManifests(ctx context.Context, hosts []hostInfo, limits []DCLimit) (stepError error) {
	w.Logger.Info(ctx, "Migrating manifest files...")
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Migrating manifest files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done migrating manifest files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	err := inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		migrationNeeded, err := w.checkV1ToV2MigrationNeeded(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Failed to check if manifest migration is needed", "host", h.IP, "error", err)
			return parallel.Abort(err)
		}

		if migrationNeeded {
			if err := w.migrateHostManifests(ctx, h); err != nil {
				w.Logger.Error(ctx, "Migrating manifest file failed", "host", h.IP, "error", err)
				return parallel.Abort(err)
			}
			if err := w.uploadMetadataVersionFile(ctx, h, "v2"); err != nil {
				w.Logger.Error(ctx, "Uploading metadata version file failed", "host", h.IP, "error", err)
				return err
			}
		}

		return nil
	})

	return err
}

// checkV1ToV2MigrationNeeded returns if V1 migration is needed.
// Decision is based on checking metadata version file located at node level
// directory.
func (w *worker) checkV1ToV2MigrationNeeded(ctx context.Context, h hostInfo) (bool, error) {
	version, err := getMetadataVersion(ctx, h.IP, h.Location, w.Client, w.ClusterID, h.DC, h.ID)
	if err != nil {
		return false, err
	}

	switch version {
	case "v1":
		return true, nil
	case "v2":
		return false, nil
	default:
		return false, errors.Errorf("not supported metadata version: %s", version)
	}
}

func (w *worker) uploadMetadataVersionFile(ctx context.Context, h hostInfo, version string) error {
	p := h.Location.RemotePath(remoteMetaVersionFile(w.ClusterID, h.DC, h.ID))

	mv := struct {
		Version string `json:"version"`
	}{Version: version}

	buf, err := json.Marshal(mv)
	if err != nil {
		return err
	}

	return w.Client.RclonePut(ctx, h.IP, p, bytes.NewReader(buf), int64(len(buf)))
}

// migrateHostManifests migrates all V1 manifests into V2 format.
func (w *worker) migrateHostManifests(ctx context.Context, h hostInfo) error {
	helper := newManifestV1Helper(h.IP, h.Location, w.Client, w.Logger.Named("v1"))

	filter := ListFilter{
		ClusterID: w.ClusterID,
		DC:        h.DC,
		NodeID:    h.ID,
	}
	manifests, err := helper.ListManifests(ctx, filter)
	if err != nil {
		return err
	}

	snapshotMapping := make(map[string]*remoteManifest)
	for _, m := range manifests {
		if _, ok := snapshotMapping[m.SnapshotTag]; !ok {
			snapshotMapping[m.SnapshotTag] = &remoteManifest{
				Location:    m.Location,
				DC:          m.DC,
				ClusterID:   m.ClusterID,
				NodeID:      m.NodeID,
				TaskID:      m.TaskID,
				SnapshotTag: m.SnapshotTag,
				Content: manifestContent{
					Version: "v1",
				},
			}
		}
		rm := snapshotMapping[m.SnapshotTag]
		rm.Content.Index = append(rm.Content.Index, m.Content.Index...)
	}

	for _, m := range snapshotMapping {
		w.Logger.Info(ctx, "Creating v2 manifest", "host", h.IP, "snapshot_tag", m.SnapshotTag)
		if _, err := w.uploadHostManifest(ctx, h, m); err != nil {
			return errors.Wrap(err, "upload migrated manifest")
		}
	}

	return nil
}
