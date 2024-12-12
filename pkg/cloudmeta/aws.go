// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"
)

// AWSMetadata is a wrapper around ec2 metadata client.
type AWSMetadata struct {
	ec2meta *ec2metadata.EC2Metadata
}

// NewAWSMetadata is a constructor for  AWSMetadata service.
func NewAWSMetadata() (*AWSMetadata, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "session.NewSession")
	}
	return &AWSMetadata{
		ec2meta: ec2metadata.New(session),
	}, nil
}

// NewAWSMetadataWithEndpoint is a constructor for  AWSMetadata service that allows your to overwrite default metadata endpoint.
// Might be useful for tests.
func NewAWSMetadataWithEndpoint(endpoint string) (*AWSMetadata, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "session.NewSession")
	}
	cfg := aws.NewConfig()
	if endpoint != "" {
		cfg = cfg.WithEndpoint(endpoint)
	}
	return &AWSMetadata{
		ec2meta: ec2metadata.New(session, cfg),
	}, nil
}

// Metadata return InstanceMetadata from aws if available.
func (aws *AWSMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
	if !aws.ec2meta.AvailableWithContext(ctx) {
		return InstanceMetadata{}, errors.New("metadata is not available")
	}

	instanceData, err := aws.ec2meta.GetInstanceIdentityDocumentWithContext(ctx)
	if err != nil {
		return InstanceMetadata{}, errors.Wrap(err, "aws.metadataClient.GetInstanceIdentityDocument")
	}

	return InstanceMetadata{
		CloudProvider: CloudProviderAWS,
		InstanceType:  instanceData.InstanceType,
	}, nil
}
