// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
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

func TestNodeInfoSupportsAlternatorQuery(t *testing.T) {
	t.Parallel()

	table := []struct {
		Version string
		Golden  bool
	}{
		{
			Version: "2019.1.2-0.20190814.2772d52",
			Golden:  false,
		},
		{
			Version: "3.1.0-0.20191012.9c3cdded9",
			Golden:  false,
		},
		{
			Version: "3.2.2-0.20200222.0b23e7145d0",
			Golden:  false,
		},
		{
			Version: "666.development",
			Golden:  true,
		},
		{
			Version: "9999.enterprise_dev",
			Golden:  true,
		},
		{
			Version: "3.3.rc2",
			Golden:  false,
		},
		{
			Version: "3.1.hotfix",
			Golden:  false,
		},
		{
			Version: "3.0.rc8",
			Golden:  false,
		},
		{
			Version: "2019.1.1-2.reader_concurrency_semaphore.20190730.f0071c669",
			Golden:  false,
		},
		{
			Version: "2019.1.5-2.many_tables.20200311.be960ed96",
			Golden:  false,
		},
		{
			Version: "4.0.0",
			Golden:  false,
		},
		{
			Version: "4.1.0",
			Golden:  true,
		},
		{
			Version: "2020.1",
			Golden:  false,
		},
		{
			Version: "2021.1",
			Golden:  false,
		},
	}

	for i := range table {
		test := table[i]

		supports, err := scyllaclient.NodeInfo{ScyllaVersion: test.Version}.SupportsAlternatorQuery()
		if err != nil {
			t.Error(err)
		}

		if !cmp.Equal(supports, test.Golden) {
			t.Errorf("SupportsAlternatorQuery(%s) = %+v, expected %+v", test.Version, supports, test.Golden)
		}
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

func TestEqualObjectStorageEndpoints(t *testing.T) {
	testCases := []struct {
		name   string
		rclone models.NodeInfoRcloneBackendConfigS3
		scylla models.ObjectStorageEndpoint
		equal  bool
	}{
		{
			name: "simple ipv4",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://192.168.200.99:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "ipv4 no schema no port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "192.168.200.99",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
			},
			equal: true,
		},
		{
			name: "simple ipv6",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://[2001:0DB9:200::99]:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "2001:0DB9:200::99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "ipv6 no schema no port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "2001:0DB9:200::99",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "2001:0DB9:200::99",
			},
			equal: true,
		},
		{
			name: "simple dns",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://s3.us-east-1.amazonaws.com:443",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "s3.us-east-1.amazonaws.com",
				Port:       443,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "dns no schema no port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "s3.us-east-1.amazonaws.com",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "s3.us-east-1.amazonaws.com",
			},
			equal: true,
		},
		{
			name: "default dns",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "",
				Region:   "us-east-1",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "s3.us-east-1.amazonaws.com",
				Port:       443,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "ipv6 with brackets",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://[2001:0DB9:200::99]:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "[2001:0DB9:200::99]",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "ipv6 with mixed brackets no schema no port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "2001:0DB9:200::99",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "[2001:0DB9:200::99]",
			},
			equal: true,
		},
		{
			name: "missing rclone port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://192.168.200.99",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "missing rclone scheme",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "192.168.200.99:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: true,
		},
		{
			name: "different name",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://192.168.200.100:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: false,
		},
		{
			name: "different port",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "https://192.168.200.99:9001",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: false,
		},
		{
			name: "different scheme",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "http://192.168.200.99:9000",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: false,
		},
		{
			name: "all different",
			rclone: models.NodeInfoRcloneBackendConfigS3{
				Endpoint: "http://192.168.200.100:9001",
				Region:   "mock",
			},
			scylla: models.ObjectStorageEndpoint{
				AwsRegion:  "mock",
				IamRoleArn: "mock",
				Name:       "192.168.200.99",
				Port:       9000,
				UseHTTPS:   true,
			},
			equal: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ok := scyllaclient.EqualObjectStorageEndpoints(tc.rclone, tc.scylla)
			if ok != tc.equal {
				t.Fatalf("Expected %v, got %v", tc.equal, ok)
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
