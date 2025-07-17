// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

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
