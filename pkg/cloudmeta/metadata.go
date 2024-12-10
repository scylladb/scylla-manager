// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"errors"
	"time"
)

// InstanceMetadata represents metadata returned by cloud provider.
type InstanceMetadata struct {
	InstanceType  string
	CloudProvider CloudProvider
}

// CloudProvider is enum of supported cloud providers.
type CloudProvider string

// CloudProviderAWS represents aws provider.
var CloudProviderAWS CloudProvider = "aws"

// CloudMetadataProvider interface that each metadata provider should implement.
type CloudMetadataProvider interface {
	Metadata(ctx context.Context) (InstanceMetadata, error)
}

// CloudMeta is a wrapper around various cloud metadata providers.
type CloudMeta struct {
	providers []CloudMetadataProvider

	providerTimeout time.Duration
}

// NewCloudMeta creates new CloudMeta provider.
func NewCloudMeta() (*CloudMeta, error) {
	// providers will initialized here and added to CloudMeta.providers.

	const defaultTimeout = 5 * time.Second

	return &CloudMeta{
		providers:       []CloudMetadataProvider{},
		providerTimeout: defaultTimeout,
	}, nil
}

// GetInstanceMetadata tries to fetch instance metadata from AWS, GCP, Azure providers in order.
func (cloud *CloudMeta) GetInstanceMetadata(ctx context.Context) (InstanceMetadata, error) {
	var mErr error
	for _, provider := range cloud.providers {
		meta, err := cloud.runWithTimeout(ctx, provider)
		if err != nil {
			mErr = errors.Join(mErr, err)
			continue
		}
		return meta, nil
	}

	return InstanceMetadata{}, mErr
}

func (cloud *CloudMeta) runWithTimeout(ctx context.Context, provider CloudMetadataProvider) (InstanceMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, cloud.providerTimeout)
	defer cancel()

	return provider.Metadata(ctx)
}

// WithProviderTimeout sets per provider timeout.
func (cloud *CloudMeta) WithProviderTimeout(providerTimeout time.Duration) {
	cloud.providerTimeout = providerTimeout
}
