// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"time"

	"github.com/scylladb/go-log"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

// InstanceMetadata represents metadata returned by cloud provider.
type InstanceMetadata struct {
	InstanceType  string
	CloudProvider CloudProvider
}

// CloudProvider is enum of supported cloud providers.
type CloudProvider string

const (
	// CloudProviderAWS represents aws provider.
	CloudProviderAWS CloudProvider = "aws"
	// CloudProviderGCP represents gcp provider.
	CloudProviderGCP CloudProvider = "gcp"
	// CloudProviderAzure represents azure provider.
	CloudProviderAzure CloudProvider = "azure"
)

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
func NewCloudMeta(logger log.Logger) (*CloudMeta, error) {
	const defaultTimeout = 5 * time.Second

	awsMeta, err := newAWSMetadata()
	if err != nil {
		return nil, err
	}

	gcpMeta := newGCPMetadata()

	azureMeta := newAzureMetadata(logger)

	return &CloudMeta{
		providers: []CloudMetadataProvider{
			awsMeta,
			gcpMeta,
			azureMeta,
		},
		providerTimeout: defaultTimeout,
	}, nil
}

// ErrNoProviders will be returned by CloudMeta service, when it hasn't been initialized with any metadata provider.
var ErrNoProviders = errors.New("no metadata providers found")

// GetInstanceMetadata tries to fetch instance metadata from AWS, GCP, Azure concurrently and returns first result.
func (cloud *CloudMeta) GetInstanceMetadata(ctx context.Context) (InstanceMetadata, error) {
	if len(cloud.providers) == 0 {
		return InstanceMetadata{}, errors.WithStack(ErrNoProviders)
	}

	type msg struct {
		meta InstanceMetadata
		err  error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan msg, len(cloud.providers))
	for _, provider := range cloud.providers {
		go func(provider CloudMetadataProvider) {
			meta, err := cloud.runWithTimeout(ctx, provider)

			select {
			case <-ctx.Done():
				return
			case results <- msg{meta: meta, err: err}:
			}
		}(provider)
	}

	// Return the first non error result or wait until all providers return err.
	var mErr error
	for range len(cloud.providers) {
		res := <-results
		if res.err != nil {
			mErr = multierr.Append(mErr, res.err)
			continue
		}
		return res.meta, nil
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
