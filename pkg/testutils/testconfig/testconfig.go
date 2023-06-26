// Copyright (C) 2017 ScyllaDB

package testconfig

import (
	"flag"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples of scylla manager db hosts")

	flagTimeout      = flag.Duration("gocql.timeout", 10*time.Second, "sets the connection `timeout` for all operations")
	flagPort         = flag.Int("gocql.port", 9042, "sets the port used to connect to the database cluster")
	flagUser         = flag.String("user", "", "CQL user")
	flagPassword     = flag.String("password", "", "CQL password")
	flagCAFile       = flag.String("ssl-ca-file", "", "Certificate Authority file")
	flagUserCertFile = flag.String("ssl-cert-file", "", "User SSL certificate file")
	flagUserKeyFile  = flag.String("ssl-key-file", "", "User SSL key file")
	flagValidate     = flag.Bool("ssl-validate", false, "Enable host verification")

	flagManagedCluster       = flag.String("managed-cluster", "127.0.0.1", "a comma-separated list of host:port tuples of data cluster hosts")
	flagManagedSecondCluster = flag.String("managed-second-cluster", "127.0.0.1", "a comma-separated list of host:port tuples of data second cluster hosts")
	flagTestNet              = flag.String("test-network", "192.168.200.", "a network where test nodes are residing")
)

// ManagedClusterHosts specifies addresses of nodes in a test cluster.
func ManagedClusterHosts() []string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return strings.Split(*flagManagedCluster, ",")
}

// IPFromTestNet returns IP from the host network:
//
//	IPFromTestNet("11") -> 192.168.200.11
//	IPFromTestNet("11") -> 2001:0DB9:200::11.
func IPFromTestNet(hostIPending string) string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagTestNet + hostIPending
}

// ManagedClusterHost returns ManagedClusterHosts()[0].
func ManagedClusterHost() string {
	s := ManagedClusterHosts()
	if len(s) == 0 {
		panic("No nodes specified in --managed-cluster flag")
	}
	return s[0]
}

// ManagedSecondClusterHosts specifies addresses of nodes in a test second cluster.
func ManagedSecondClusterHosts() []string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return strings.Split(*flagManagedSecondCluster, ",")
}

// ManagedClusterCredentials returns CQL username and password.
func ManagedClusterCredentials() (user, password string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagUser, *flagPassword
}

// TestDBUsername returns '--username' flag value.
func TestDBUsername() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagUser
}

// TestDBPassword returns '--password' flag value.
func TestDBPassword() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagPassword
}

// CQLPort returns port where scylla is listening CQL connections.
func CQLPort() int {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagPort
}

// CQLSSLOptions returns ssl options created from test command line.
func CQLSSLOptions() *gocql.SslOptions {
	if !flag.Parsed() {
		flag.Parse()
	}
	return &gocql.SslOptions{
		CaPath:                 *flagCAFile,
		CertPath:               *flagUserCertFile,
		KeyPath:                *flagUserKeyFile,
		EnableHostVerification: *flagValidate,
	}
}

// CQLTimeout returns timeout for cql session.
func CQLTimeout() time.Duration {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagTimeout
}

// ScyllaManagerDBCluster return scylla node address of sm cluster.
func ScyllaManagerDBCluster() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagCluster
}
