// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"

	"github.com/pkg/errors"
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

	return "", errors.Errorf("cannot guess Scylla broadcast/listen address please set https property in configuration file %s available IP addresses are %s", rootArgs.configFile, addrs)
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
			if !ok {
				continue // should never happen
			}
			addrTable = append(addrTable, ip.IP.String())
		}
	}

	return addrTable
}
