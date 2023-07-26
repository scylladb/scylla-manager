// Copyright (C) 2022 ScyllaDB

package testutils

import (
	"net"
	"strings"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

// URLEncodeIP converts ip to URL representation:
// URLEncodeIP("192.168.1.1") -> "192.168.1.1"
// URLEncodeIP("100:200::1") -> "[100:200::1]".
func URLEncodeIP(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}

// IsPublicIP returns true if ip belongs to the public network.
func IsPublicIP(host string) bool {
	return isPublicIP(host, testconfig.IPFromTestNet("1"))
}

// IsPublicIP returns true if ip belongs to the public network.
func isPublicIP(host, publicNetIP string) bool {
	var chunks []string
	canonicalIP := ToCanonicalIP(publicNetIP)
	var sep string
	if strings.Contains(canonicalIP, ":") {
		sep = ":"
	} else {
		sep = "."
	}
	chunks = strings.Split(canonicalIP, sep)
	canonicalNet := strings.Join(chunks[0:len(chunks)-1], sep)
	return strings.HasPrefix(net.ParseIP(host).String(), canonicalNet)
}

// IsIPV6Network returns true if test is running on IPV6.
func IsIPV6Network() bool {
	return strings.Contains(testconfig.IPFromTestNet("1"), ":")
}

// IPBelongsToDC returns true if ip belongs to given DC.
func IPBelongsToDC(dc, host string) bool {
	var hostEnding string
	switch dc {
	case "dc1":
		hostEnding = "1"
	case "dc2":
		hostEnding = "2"
	default:
		return false
	}
	return strings.HasPrefix(
		net.ParseIP(host).String(),
		net.ParseIP(testconfig.IPFromTestNet(hostEnding)).String())
}

// ToCanonicalIP replaces ":0:0" in IPv6 addresses with "::"
// ToCanonicalIP("192.168.0.1") -> "192.168.0.1"
// ToCanonicalIP("100:200:0:0:0:0:0:1") -> "100:200::1".
func ToCanonicalIP(host string) string {
	val := net.ParseIP(host)
	if val == nil {
		return host
	}
	return val.String()
}

// ExpandIP expands IPv6 addresses "::" to ":0:0..."
// ToCanonicalIP("192.168.0.1") -> "192.168.0.1"
// ToCanonicalIP("100:200:0:0:0:0:0:1") -> "100:200::1".
func ExpandIP(host string) string {
	if !strings.Contains(host, "::") {
		return host
	}
	expected := 7
	existing := strings.Count(host, ":") - 1
	return strings.Replace(host, "::", strings.Repeat(":0", expected-existing)+":", 1)
}
