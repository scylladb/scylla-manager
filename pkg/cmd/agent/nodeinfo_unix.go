// Copyright (C) 2017 ScyllaDB
//go:build !linux
// +build !linux

package main

import (
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

// sysInfo is a no-op implementation for all systems but Linux.
// This is intended for development purposes only, Scylla Manager supports only Linux OS.
func (h *nodeInfoHandler) sysInfo(info *scyllaclient.NodeInfo) error {
	info.MemoryTotal = 0
	info.CPUCount = 0
	info.Uptime = 0
	return nil
}
