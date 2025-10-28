// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

const fallback = "4.3.2.1"

func TestNodeInfoCQLAddr(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name              string
		NodeInfo          *scyllaclient.NodeInfo
		ClusterDisableSSL bool
		GoldenAddress     string
	}{
		{
			Name: "Broadcast RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				BroadcastRPCAddress: "1.2.3.4",
				RPCAddress:          "1.2.3.5",
				ListenAddress:       "1.2.3.6",
				NativeTransportPort: "1234",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.4:1234",
		},
		{
			Name: "RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "1.2.3.5",
				ListenAddress:       "1.2.3.6",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.5:1234",
		},
		{
			Name: "Listen Address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "1.2.3.6",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.6:1234",
		},
		{
			Name: "Fallback is returned when RPC Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "0.0.0.0",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when RPC Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "::0",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "0.0.0.0",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "::0",
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			addr := test.NodeInfo.CQLAddr(fallback, test.ClusterDisableSSL)
			if addr != test.GoldenAddress {
				t.Errorf("expected %s address, got %s", test.GoldenAddress, addr)
			}
		})
	}
}

func TestNodeInfoCQLSSLAddr(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name              string
		NodeInfo          *scyllaclient.NodeInfo
		ClusterDisableSSL bool
		GoldenAddress     string
	}{
		{
			Name: "Broadcast RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				BroadcastRPCAddress:     "1.2.3.4",
				RPCAddress:              "1.2.3.5",
				ListenAddress:           "1.2.3.6",
				NativeTransportPortSsl:  "1234",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.4:1234",
		},
		{
			Name: "RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				RPCAddress:              "1.2.3.5",
				ListenAddress:           "1.2.3.6",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.5:1234",
		},
		{
			Name: "Listen Address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "1.2.3.6",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.6:1234",
		},
		{
			Name: "Fallback is returned when RPC Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				RPCAddress:              "0.0.0.0",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when RPC Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				RPCAddress:              "::0",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "0.0.0.0",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "::0",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "NativeTransportPort is returned when ssl is disabled on cluster level",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort:     "4321",
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "1.2.3.4",
				ClientEncryptionEnabled: true,
			},
			ClusterDisableSSL: true,
			GoldenAddress:     "1.2.3.4:4321",
		},
		{
			Name: "NativeTransportPort is returned when Node Client Encryption is not enabled",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort:     "4321",
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "1.2.3.4",
				ClientEncryptionEnabled: false,
			},
			ClusterDisableSSL: false,
			GoldenAddress:     "1.2.3.4:4321",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			addr := test.NodeInfo.CQLAddr(fallback, test.ClusterDisableSSL)
			if addr != test.GoldenAddress {
				t.Errorf("expected %s address, got %s", test.GoldenAddress, addr)
			}
		})
	}
}

func TestNodeInfoSupportsRepairSmallTableOptimization(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		scyllaVer string
		expected  bool
	}{
		{
			scyllaVer: "2024.1.4",
			expected:  false,
		},
		{
			scyllaVer: "2024.2.5",
			expected:  true,
		},
		{
			scyllaVer: "2024.2.6",
			expected:  true,
		},
		{
			scyllaVer: "5.4.9",
			expected:  false,
		},
		{
			scyllaVer: "5.5.0",
			expected:  false,
		},
		{
			scyllaVer: "6.0.0",
			expected:  true,
		},
		{
			scyllaVer: "6.0.1",
			expected:  true,
		},
		{
			scyllaVer: "6.1.0",
			expected:  true,
		},
	}

	for _, tc := range testCases {
		ni := scyllaclient.NodeInfo{
			ScyllaVersion: tc.scyllaVer,
		}
		result, err := ni.SupportsRepairSmallTableOptimization()
		if err != nil {
			t.Fatal(err)
		}
		if result != tc.expected {
			t.Fatalf("expected {%v}, but got {%v}, version = {%s}", tc.expected, result, tc.scyllaVer)
		}
	}
}

func TestSupportsSafeDescribeSchemaWithInternals(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		scyllaVersion  string
		expectedMethod scyllaclient.SafeDescribeMethod
		expectedError  error
	}{
		{
			name:           "when scylla >= 2025.1, then it is expected to support read barrier api",
			scyllaVersion:  "2025.1.0-candidate-20241106103631",
			expectedMethod: scyllaclient.SafeDescribeMethodReadBarrierAPI,
			expectedError:  nil,
		},
		{
			name:           "when scylla >= 6.1, then it is expected to support read barrier api",
			scyllaVersion:  "6.2.1-candidate-20241106103631",
			expectedMethod: scyllaclient.SafeDescribeMethodReadBarrierAPI,
			expectedError:  nil,
		},
		{
			name:           "when scylla >= 2024.2, then it is expected to support read barrier cql",
			scyllaVersion:  "2024.2",
			expectedMethod: scyllaclient.SafeDescribeMethodReadBarrierCQL,
			expectedError:  nil,
		},
		{
			name:           "when scylla >= 6.0, then it is expected to support read barrier cql",
			scyllaVersion:  "6.0.1",
			expectedMethod: scyllaclient.SafeDescribeMethodReadBarrierCQL,
			expectedError:  nil,
		},
		{
			name:           "when scylla < 6.0, then it is expected to not support any safe method",
			scyllaVersion:  "5.9.9",
			expectedMethod: "",
			expectedError:  nil,
		},
		{
			name:           "when scylla < 2024.2, then it is expected to not support any safe method",
			scyllaVersion:  "2024.1",
			expectedMethod: "",
			expectedError:  nil,
		},
		{
			name:           "when scylla version is not a semver, then it is expected to return an error",
			scyllaVersion:  "main",
			expectedMethod: "",
			expectedError:  errors.New("Unsupported Scylla version: main"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ni := scyllaclient.NodeInfo{ScyllaVersion: tc.scyllaVersion}
			method, err := ni.SupportsSafeDescribeSchemaWithInternals()
			if err != nil && tc.expectedError == nil {
				t.Fatalf("unexpected err - %v", err)
			}
			if tc.expectedError != nil && err.Error() != tc.expectedError.Error() {
				t.Fatalf("actual err != expected err, '%s' != '%s'", err.Error(), tc.expectedError.Error())
			}

			if method != tc.expectedMethod {
				t.Fatalf("actual method != expected method, %s != %s", method, tc.expectedMethod)
			}
		})
	}
}

func TestScyllaObjectStorageEndpoint(t *testing.T) {
	testCases := []struct {
		name     string
		provider backupspec.Provider
		rclone   models.NodeInfoRcloneBackendConfig
		scylla   []models.ObjectStorageEndpoint
		endpoint string
	}{
		{
			name:     "simple ipv4",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.99:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "192.168.200.99",
		},
		{
			name:     "ipv4 no schema no port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "192.168.200.99",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
				},
			},
			endpoint: "192.168.200.99",
		},
		{
			name:     "simple ipv6",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://[2001:0DB9:200::99]:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "2001:0DB9:200::99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "2001:0DB9:200::99",
		},
		{
			name:     "ipv6 no schema no port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "2001:0DB9:200::99",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "2001:0DB9:200::99",
				},
			},
			endpoint: "2001:0DB9:200::99",
		},
		{
			name:     "simple dns",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://s3.us-east-1.amazonaws.com:443",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "s3.us-east-1.amazonaws.com",
					Port:       443,
					UseHTTPS:   true,
				},
			},
			endpoint: "s3.us-east-1.amazonaws.com",
		},
		{
			name:     "dns no schema no port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "s3.us-east-1.amazonaws.com",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "s3.us-east-1.amazonaws.com",
				},
			},
			endpoint: "s3.us-east-1.amazonaws.com",
		},
		{
			name:     "default dns",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "",
					Region:   "us-east-1",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "s3.us-east-1.amazonaws.com",
					Port:       443,
					UseHTTPS:   true,
				},
			},
			endpoint: "s3.us-east-1.amazonaws.com",
		},
		{
			name:     "ipv6 with brackets",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://[2001:0DB9:200::99]:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "[2001:0DB9:200::99]",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "[2001:0DB9:200::99]",
		},
		{
			name:     "ipv6 with mixed brackets no schema no port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "2001:0DB9:200::99",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "[2001:0DB9:200::99]",
				},
			},
			endpoint: "[2001:0DB9:200::99]",
		},
		{
			name:     "missing rclone port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.99",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "192.168.200.99",
		},
		{
			name:     "missing rclone scheme",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "192.168.200.99:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "192.168.200.99",
		},
		{
			name:     "different name",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.100:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "",
		},
		{
			name:     "different port",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.99:9001",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "",
		},
		{
			name:     "different scheme",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "http://192.168.200.99:9000",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "",
		},
		{
			name:     "all different",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "http://192.168.200.100:9001",
					Region:   "mock",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "",
		},
		{
			name:     "default gs",
			provider: backupspec.GCS,
			rclone: models.NodeInfoRcloneBackendConfig{
				Gcs: models.NodeInfoRcloneBackendConfigGcs{},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					Type: "gs",
					Name: "default",
				},
			},
			endpoint: "default",
		},
		{
			name:     "custom gs",
			provider: backupspec.GCS,
			rclone: models.NodeInfoRcloneBackendConfig{
				Gcs: models.NodeInfoRcloneBackendConfigGcs{
					Endpoint: "https://127.0.0.1:4443",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					Type: "gs",
					Name: "https://127.0.0.1:4443",
				},
			},
			endpoint: "https://127.0.0.1:4443",
		},
		{
			name:     "different endpoint gs",
			provider: backupspec.GCS,
			rclone: models.NodeInfoRcloneBackendConfig{
				Gcs: models.NodeInfoRcloneBackendConfigGcs{
					Endpoint: "https://127.0.0.1:4443",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					Type: "gs",
					Name: "https://127.0.0.2:4443",
				},
			},
			endpoint: "",
		},
		{
			name:     "multiple endpoints s3",
			provider: backupspec.S3,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.100:9000",
					Region:   "mock",
				},
				Gcs: models.NodeInfoRcloneBackendConfigGcs{
					Endpoint: "https://127.0.0.1:4443",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.99",
					Port:       9000,
					UseHTTPS:   true,
				},
				{
					Type: "gs",
					Name: "http://192.168.200.100:9000",
				},
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "192.168.200.100",
					Port:       9000,
					UseHTTPS:   true,
				},
			},
			endpoint: "192.168.200.100",
		},
		{
			name:     "multiple endpoints gs",
			provider: backupspec.GCS,
			rclone: models.NodeInfoRcloneBackendConfig{
				S3: models.NodeInfoRcloneBackendConfigS3{
					Endpoint: "https://192.168.200.100:9000",
					Region:   "mock",
				},
				Gcs: models.NodeInfoRcloneBackendConfigGcs{
					Endpoint: "https://127.0.0.1:4443",
				},
			},
			scylla: []models.ObjectStorageEndpoint{
				{
					Type: "gs",
					Name: "default",
				},
				{
					AwsRegion:  "mock",
					IamRoleArn: "mock",
					Name:       "127.0.0.1",
					Port:       4443,
					UseHTTPS:   true,
				},
				{
					Type: "gs",
					Name: "https://127.0.0.1:4443",
				},
			},
			endpoint: "https://127.0.0.1:4443",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ose := make(map[string]models.ObjectStorageEndpoint)
			for _, e := range tc.scylla {
				ose[e.Name] = e
			}
			ni := scyllaclient.NodeInfo{
				RcloneBackendConfig:    tc.rclone,
				ObjectStorageEndpoints: ose,
			}
			endpoint, err := ni.ScyllaObjectStorageEndpoint(tc.provider)
			if tc.endpoint == "" {
				if err == nil {
					t.Fatalf("Expected no matching endpoint and error, got: %v, %v", endpoint, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got: %v", err)
				}
				if tc.endpoint != endpoint {
					t.Fatalf("Expected matching endpoint %q, got %q", tc.endpoint, endpoint)
				}
			}
		})
	}
}

func TestNodeInfoSupportsSkipCleanupAndSkipReshape(t *testing.T) {
	testCases := []struct {
		name          string
		scyllaVersion string
		expected      bool
	}{
		{
			name:          "master version is supported",
			scyllaVersion: "9999.enterprise_dev",
			expected:      true,
		},
		{
			name:          "2025.3.0 is supported",
			scyllaVersion: "2025.3.0",
			expected:      true,
		},
		{
			name:          "2025.2.1 is supported",
			scyllaVersion: "2025.2.1",
			expected:      true,
		},
		{
			name:          "2025.2.0 is not supported",
			scyllaVersion: "2025.2.0",
			expected:      false,
		},
		{
			name:          "2025.1.0 is not supported",
			scyllaVersion: "2025.1.0",
			expected:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ni := scyllaclient.NodeInfo{ScyllaVersion: tc.scyllaVersion}
			supported, err := ni.SupportsSkipCleanupAndSkipReshape()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if supported != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, supported)
			}
		})
	}
}
