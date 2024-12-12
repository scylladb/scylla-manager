// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"github.com/pkg/errors"
)

// GCPMetadata is a wrapper around gcp metadata client.
type GCPMetadata struct {
	meta *metadata.Client
}

// NewGCPMetadata returns gcp metadata provider.
func NewGCPMetadata() *GCPMetadata {
	return &GCPMetadata{
		meta: metadata.NewClient(nil),
	}
}

// Metadata returns InstanceMetadata from gcp if available.
func (gcp *GCPMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
	machineType, err := gcp.getMachineType(ctx)
	if err != nil {
		return InstanceMetadata{}, errors.Wrap(err, "gcp.meta.GetWithContext")
	}
	return InstanceMetadata{
		CloudProvider: CloudProviderGCP,
		InstanceType:  machineType,
	}, nil
}

func (gcp *GCPMetadata) getMachineType(ctx context.Context) (string, error) {
	// The machine type for this VM. This value has the following format: projects/PROJECT_NUM/machineTypes/MACHINE_TYPE.
	machineType, err := gcp.meta.GetWithContext(ctx, "instance/machine-type")
	if err != nil {
		return "", errors.Wrap(err, "gcp.meta.GetWithContext")
	}

	parts := strings.Split(machineType, "/")
	if len(parts) < 2 {
		return "", errors.Errorf("unexpected machine-type format: %s", machineType)
	}

	return parts[len(parts)-1], nil
}
