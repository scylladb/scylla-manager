// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"
)

// awsMetadata is a wrapper around ec2 metadata client.
type awsMetadata struct {
	ec2meta *ec2metadata.EC2Metadata
}

// newAWSMetadata is a constructor for  AWSMetadata service.
func newAWSMetadata() (*awsMetadata, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "session.NewSession")
	}
	return &awsMetadata{
		ec2meta: ec2metadata.New(session),
	}, nil
}

// Metadata return InstanceMetadata from aws if available.
func (aws *awsMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
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
