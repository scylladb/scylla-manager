// Copyright (C) 2017 ScyllaDB

package netutil

import "net"

// IsIPv4 returns true iff IP is version 4.
// It supports both IPs of length 4 and 16 that is IPv6 encoding of IPv4.
func IsIPv4(ip net.IP) bool {
	return len(ip) == net.IPv4len ||
		len(ip) == net.IPv6len &&
			isZeros(ip[0:10]) &&
			ip[10] == 0xff &&
			ip[11] == 0xff
}

// Is p all zeros?
func isZeros(p net.IP) bool {
	for i := 0; i < len(p); i++ {
		if p[i] != 0 {
			return false
		}
	}
	return true
}
