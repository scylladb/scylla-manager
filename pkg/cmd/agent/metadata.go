// Copyright (C) 2024 ScyllaDB

package main

import (
	"context"
	"net/http"
	"sync"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/cloudmeta"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

func newMetadataHandler(logger log.Logger) http.HandlerFunc {
	var (
		m        sync.Mutex
		loaded   bool
		metadata cloudmeta.InstanceMetadata
	)

	// Caches only successful result of GetInstanceMetadata.
	lazyGetMetadata := func(ctx context.Context) (cloudmeta.InstanceMetadata, error) {
		m.Lock()
		defer m.Unlock()
		if loaded {
			return metadata, nil
		}

		metaSvc, err := cloudmeta.NewCloudMeta(logger)
		if err != nil {
			return cloudmeta.InstanceMetadata{}, errors.Wrap(err, "NewCloudMeta")
		}

		metadata, err = metaSvc.GetInstanceMetadata(ctx)
		if err != nil {
			return cloudmeta.InstanceMetadata{}, err
		}

		loaded = true
		return metadata, nil
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		instanceMeta, err := lazyGetMetadata(ctx)
		if err != nil {
			// Metadata may not be available for several reasons:
			// 1. running on-premise 2. disabled 3. smth went wrong with metadata server.
			// As we cannot distinguish between these cases, we can only log err.
			logger.Error(ctx, "GetInstanceMetadata", "err", err)
			render.Respond(w, r, models.InstanceMetadata{})
			return
		}
		render.Respond(w, r, models.InstanceMetadata{
			CloudProvider: string(instanceMeta.CloudProvider),
			InstanceType:  instanceMeta.InstanceType,
		})
	}
}
