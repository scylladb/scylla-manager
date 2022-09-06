// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

// restoreHost represents host that can be used for restoring files.
// If set, OngoingRunProgress represents unfinished RestoreRunProgress created in previous run.
type restoreHost struct {
	Host               string
	Shards             uint
	OngoingRunProgress *RestoreRunProgress
}

// bundle represents SSTables with the same ID.
type bundle []string

// restoreWorker is responsible for coordinating restore procedure.
type restoreWorker struct {
	workerTools

	// Fields below are constant among all restore runs of the same restore task.
	managerSession gocqlx.Session
	clusterSession gocqlx.Session
	// Iterates over all manifests in given location with
	// cluster ID and snapshot tag specified in restore target.
	forEachRestoredManifest func(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error

	// Fields below are mutable for each restore run
	location     Location                // Currently restored location
	miwc         ManifestInfoWithContent // Currently restored manifest
	hosts        []restoreHost           // Restore units created for currently restored location
	bundles      map[string]bundle       // Maps bundle to it's ID
	bundleIDPool chan string             // IDs of the bundles that are yet to be restored
	resumed      bool                    // Set to true if current run has already skipped all tables restored in previous run
}
