// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"github.com/pkg/errors"
)

// gcpMetadata is a wrapper around gcp metadata client.
type gcpMetadata struct {
	meta *metadata.Client
}

// newGCPMetadata returns gcp metadata provider.
func newGCPMetadata() *gcpMetadata {
	return &gcpMetadata{
		meta: metadata.NewClient(nil),
	}
}

// Metadata returns InstanceMetadata from gcp if available.
func (gcp *gcpMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
	machineType, err := gcp.getMachineType(ctx)
	if err != nil {
		return InstanceMetadata{}, errors.Wrap(err, "gcp.meta.GetWithContext")
	}
	return InstanceMetadata{
		CloudProvider: CloudProviderGCP,
		InstanceType:  machineType,
	}, nil
}

func (gcp *gcpMetadata) getMachineType(ctx context.Context) (string, error) {
	machineTypeResp, err := gcp.meta.GetWithContext(ctx, "instance/machine-type")
	if err != nil {
		return "", errors.Wrap(err, "gcp.meta.GetWithContext")
	}

	machineType, err := parseMachineTypeResponse(machineTypeResp)
	if err != nil {
		return "", err
	}

	return machineType, nil
}

// The machine type for this VM. This value has the following format: projects/PROJECT_NUM/machineTypes/MACHINE_TYPE.
// See https://cloud.google.com/compute/docs/metadata/predefined-metadata-keys#instance-metadata.
func parseMachineTypeResponse(resp string) (string, error) {
	errUnexpectedFormat := errors.Errorf("unexpected machineType response format: %s", resp)

	parts := strings.Split(resp, "/")
	if len(parts) != 4 {
		return "", errUnexpectedFormat
	}

	if parts[2] != "machineTypes" {
		return "", errUnexpectedFormat
	}

	return parts[3], nil
}
