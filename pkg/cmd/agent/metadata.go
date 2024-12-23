// Copyright (C) 2024 ScyllaDB

package main

import (
	"context"
	"net/http"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/cloudmeta"
)

type metadataResponse struct {
	CloudProvider string `json:"cloud_provider"`
	InstanceType  string `json:"instance_type"`
}

func newMetadataHandler(logger log.Logger) (http.HandlerFunc, error) {
	ctx := context.Background()
	metaSvc, err := cloudmeta.NewCloudMeta(logger)
	if err != nil {
		return nil, errors.Wrap(err, "NewCloudMeta")
	}

	instanceMeta, err := metaSvc.GetInstanceMetadata(ctx)
	if err != nil {
		// Metadata may not be available for several reasons:
		// 1. running on-premise 2. disabled 3. smth went wrong with metadata server.
		// As we cannot distinguish between these cases, we can only log err.
		logger.Error(ctx, "GetInstanceMetadata", "err", err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		render.Respond(w, r, metadataResponse{
			CloudProvider: string(instanceMeta.CloudProvider),
			InstanceType:  instanceMeta.InstanceType,
		})
	}, nil
}
