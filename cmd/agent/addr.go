// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/netutil"
)

func httpsListenAddr(c scyllaConfig) (string, error) {
	if c.BroadcastAddress != "" {
		return c.BroadcastAddress, nil
	}
	// Ignore listen address "localhost" it's the default value in scylla.yaml
	// most probably the real listen address is passed on command line like in
	// scylladb/scylla docker image for instance.
	if c.ListenAddress != "" && c.ListenAddress != "localhost" {
		return c.ListenAddress, nil
	}

	addrs := guessAddr()
	if len(addrs) == 1 {
		return addrs[0], nil
	}

	for i := range addrs {
		addrs[i] += ":" + defaultHTTPSPort
	}

	return "", errors.Errorf("cannot guess Scylla broadcast address, "+
		"please set https config option to one of: %s", strings.Join(addrs, ", "))
}

func guessAddr() []string {
	ifces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	var addrTable []string
	for _, ifce := range ifces {
		// Skip downed, loopback and p2p interfaces
		if ifce.Flags&(net.FlagUp|net.FlagLoopback|net.FlagPointToPoint) != net.FlagUp {
			continue
		}
		addrs, err := ifce.Addrs()
		if err != nil {
			continue // ignore error
		}
		for _, addr := range addrs {
			ip, ok := addr.(*net.IPNet)
			if !ok || !netutil.IsIPv4(ip.IP) {
				continue // should never happen
			}
			addrTable = append(addrTable, ip.IP.String())
		}
	}

	return addrTable
}
