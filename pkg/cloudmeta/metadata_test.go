// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetInstanceMetadata(t *testing.T) {
	testCases := []struct {
		name      string
		providers []CloudMetadataProvider

		expectedErr  bool
		expectedMeta InstanceMetadata
	}{
		{
			name:      "when there is no active providers",
			providers: nil,

			expectedErr:  true,
			expectedMeta: InstanceMetadata{},
		},
		{
			name: "when there is one active providers",
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", 1*time.Millisecond, nil),
			},

			expectedErr: false,
			expectedMeta: InstanceMetadata{
				CloudProvider: "test_provider_1",
				InstanceType:  "x-test-1",
			},
		},
		{
			name: "when there is more than one active provider, fastest should be returned",
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", 1*time.Millisecond, nil),
				newTestProvider(t, "test_provider_2", "x-test-2", 100*time.Millisecond, nil),
			},

			expectedErr: false,
			expectedMeta: InstanceMetadata{
				CloudProvider: "test_provider_1",
				InstanceType:  "x-test-1",
			},
		},
		{
			name: "when there is more than one active provider, but fastest returns err",
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", 1*time.Millisecond, errors.New("something went wront")),
				newTestProvider(t, "test_provider_2", "x-test-2", 100*time.Millisecond, nil),
			},

			expectedErr: false,
			expectedMeta: InstanceMetadata{
				CloudProvider: "test_provider_2",
				InstanceType:  "x-test-2",
			},
		},
		{
			name: "when there is more than one active provider, but all returns err",
			providers: []CloudMetadataProvider{
				newTestProvider(t, "test_provider_1", "x-test-1", 1*time.Millisecond, errors.New("err provider1")),
				newTestProvider(t, "test_provider_2", "x-test-2", 1*time.Millisecond, errors.New("err provider2")),
			},

			expectedErr:  true,
			expectedMeta: InstanceMetadata{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cloudmeta := &CloudMeta{
				providers:       tc.providers,
				providerTimeout: 1 * time.Second,
			}

			meta, err := cloudmeta.GetInstanceMetadata(context.Background())

			if tc.expectedErr && err == nil {
				t.Fatalf("expected error, got: %v", err)
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("unexpected error, got: %v", err)
			}

			if tc.expectedMeta.InstanceType != meta.InstanceType {
				t.Fatalf("unexpected meta.InstanceType: %s != %s", tc.expectedMeta.InstanceType, meta.InstanceType)
			}

			if tc.expectedMeta.CloudProvider != meta.CloudProvider {
				t.Fatalf("unexpected meta.CloudProvider: %s != %s", tc.expectedMeta.CloudProvider, meta.CloudProvider)
			}
		})
	}
}

func TestGetInstanceMetadataWithCancelledContext(t *testing.T) {
	cloudmeta := &CloudMeta{
		providers: []CloudMetadataProvider{
			newTestProvider(t, "test_provider_1", "x-test-1", 1*time.Second, nil),
		},
		providerTimeout: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	_ = time.AfterFunc(50*time.Millisecond, cancel)

	meta, err := cloudmeta.GetInstanceMetadata(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if meta.CloudProvider != "" {
		t.Fatalf("meta.CloudProvider should be empty, got %s", meta.CloudProvider)
	}

	if meta.InstanceType != "" {
		t.Fatalf("meta.InstanceType should be empty, got %s", meta.InstanceType)
	}
}

func newTestProvider(t *testing.T, providerName, instanceType string, latency time.Duration, err error) *testProvider {
	t.Helper()

	return &testProvider{
		name:         CloudProvider(providerName),
		instanceType: instanceType,
		latency:      latency,
		err:          err,
	}
}

type testProvider struct {
	name         CloudProvider
	instanceType string
	latency      time.Duration
	err          error
}

func (tp testProvider) Metadata(ctx context.Context) (InstanceMetadata, error) {
	select {
	case <-time.After(tp.latency):
	case <-ctx.Done():
		return InstanceMetadata{}, ctx.Err()
	}

	if tp.err != nil {
		return InstanceMetadata{}, tp.err
	}
	return InstanceMetadata{
		CloudProvider: tp.name,
		InstanceType:  tp.instanceType,
	}, ctx.Err()
}
