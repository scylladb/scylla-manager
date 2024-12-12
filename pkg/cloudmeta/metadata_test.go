// Copyright (C) 2017 ScyllaDB

package cloudmeta

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestGetInstanceMetadata(t *testing.T) {
	t.Run("when there is no active providers", func(t *testing.T) {
		cloudmeta := &CloudMeta{}

		meta, err := cloudmeta.GetInstanceMetadata(context.Background())
		if !errors.Is(err, ErrNoProviders) {
			t.Fatalf("expected err, got: %v", err)
		}

		if meta.InstanceType != "" {
			t.Fatalf("meta.InstanceType should be empty, got %v", meta.InstanceType)
		}

		if meta.CloudProvider != "" {
			t.Fatalf("meta.CloudProvider should be empty, got %v", meta.CloudProvider)
		}
	})

	t.Run("when there is only one active provider", func(t *testing.T) {
		cloudmeta := &CloudMeta{
			providers: []CloudMetadataProvider{newTestProvider(t, "test_provider_1", "x-test-1", nil)},
		}

		meta, err := cloudmeta.GetInstanceMetadata(context.Background())
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if meta.InstanceType != "x-test-1" {
			t.Fatalf("meta.InstanceType should be 'x-test-1', got %v", meta.InstanceType)
		}

		if meta.CloudProvider != "test_provider_1" {
			t.Fatalf("meta.CloudProvider should be 'test_provider_1', got %v", meta.CloudProvider)
		}
	})

	t.Run("when there is more than one active provider", func(t *testing.T) {
		cloudmeta := &CloudMeta{
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", nil),
				newTestProvider(t, "test_provider_2", "x-test-2", nil),
			},
		}

		// Only first one should be returned.
		meta, err := cloudmeta.GetInstanceMetadata(context.Background())
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if meta.InstanceType != "x-test-1" {
			t.Fatalf("meta.InstanceType should be 'x-test-1', got %v", meta.InstanceType)
		}

		if meta.CloudProvider != "test_provider_1" {
			t.Fatalf("meta.CloudProvider should be 'test_provider_1', got %v", meta.CloudProvider)
		}
	})
	t.Run("when there is more than one active provider, but first returns err", func(t *testing.T) {
		cloudmeta := &CloudMeta{
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", fmt.Errorf("'test_provider_1' err")),
				newTestProvider(t, "test_provider_2", "x-test-2", nil),
			},
		}

		// Only first succesfull one should be returned.
		meta, err := cloudmeta.GetInstanceMetadata(context.Background())
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if meta.InstanceType != "x-test-2" {
			t.Fatalf("meta.InstanceType should be 'x-test-2', got %v", meta.InstanceType)
		}

		if meta.CloudProvider != "test_provider_2" {
			t.Fatalf("meta.CloudProvider should be 'test_provider_2', got %v", meta.CloudProvider)
		}
	})

	t.Run("when there is more than one active provider, but all returns err", func(t *testing.T) {
		cloudmeta := &CloudMeta{
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", fmt.Errorf("'test_provider_1' err")),
				newTestProvider(t, "test_provider_2", "x-test-2", fmt.Errorf("'test_provider_2' err")),
			},
		}

		// Only first succesfull one should be returned.
		meta, err := cloudmeta.GetInstanceMetadata(context.Background())
		if err == nil {
			t.Fatalf("expected err, but got: %v", err)
		}

		if meta.InstanceType != "" {
			t.Fatalf("meta.InstanceType should be empty, got %v", meta.InstanceType)
		}

		if meta.CloudProvider != "" {
			t.Fatalf("meta.CloudProvider should be empty, got %v", meta.CloudProvider)
		}
	})
}

func newTestProvider(t *testing.T, providerName, instanceType string, err error) *testProvider {
	t.Helper()

	return &testProvider{
		name:         CloudProvider(providerName),
		instanceType: instanceType,
		err:          err,
	}
}

type testProvider struct {
	name         CloudProvider
	instanceType string
	err          error
}

func (tp testProvider) Metadata(ctx context.Context) (InstanceMetadata, error) {
	if tp.err != nil {
		return InstanceMetadata{}, tp.err
	}
	return InstanceMetadata{
		CloudProvider: tp.name,
		InstanceType:  tp.instanceType,
	}, nil
}
