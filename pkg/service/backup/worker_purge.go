// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func (w *worker) Purge(ctx context.Context, hosts []hostInfo, policy map[uuid.UUID]int) (err error) {
	w.Logger.Info(ctx, "Purging old data...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Purging old data failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done purging old data", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	// List manifests in all locations.
	manifests, err := w.listAllManifests(ctx, hosts)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	// Get a list of stale tags.
	tags := staleTags(manifests, policy, timeutc.Now().AddDate(0, 0, -30))

	// Group manifests by node.
	nodeIDManifests := groupManifestsByNode(manifests)
	// Allow multiple Go routines select nodes (and manifests) to purge.
	var mu sync.Mutex
	pop := func(l Location) (string, []*RemoteManifest) {
		mu.Lock()
		defer mu.Unlock()
		for nodeID, manifests := range nodeIDManifests {
			if manifests[0].Location == l {
				delete(nodeIDManifests, nodeID)
				return nodeID, manifests
			}
		}
		return "", nil
	}

	return hostsInParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		var (
			nodeID    string
			manifests []*RemoteManifest
		)

		p := newPurger(w.Client, h.IP, w.Logger)
		p.OnPreDelete = func(files int) {
			host := p.Host(nodeID)
			if host == "" {
				w.Logger.Error(ctx, "Missing IP for node ID, not setting purge metrics", "node_id", nodeID)
			} else {
				w.Metrics.SetPurgeFiles(w.ClusterID, host, files)
			}
		}
		p.OnDelete = func() {
			host := p.Host(nodeID)
			if host != "" {
				w.Metrics.IncPurgeDeletedFiles(w.ClusterID, host)
			}
		}

		for {
			// Get node to purge in the same location, if cannot find any exit.
			nodeID, manifests = pop(h.Location)
			if len(manifests) == 0 {
				return nil
			}

			if _, err := p.PurgeSnapshotTags(ctx, manifests, tags); err != nil {
				return errors.Wrapf(err, "node %s", nodeID)
			}
		}
	})
}

func (w *worker) listAllManifests(ctx context.Context, hosts []hostInfo) ([]*RemoteManifest, error) {
	var (
		locations = make(map[Location]struct{})
		manifests []*RemoteManifest
	)

	for _, hi := range hosts {
		if _, ok := locations[hi.Location]; ok {
			continue
		}
		locations[hi.Location] = struct{}{}

		lm, err := listAllManifests(ctx, w.Client, hi.IP, hi.Location, w.ClusterID)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, lm...)
	}

	return manifests, nil
}
