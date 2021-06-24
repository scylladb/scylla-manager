// Copyright (C) 2017 ScyllaDB
// +build !linux

package main

import (
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

func (h *nodeInfoHandler) sysInfo(info *scyllaclient.NodeInfo) error {
	// TODO: extract correct data from other supported unix platforms
	info.MemoryTotal = 0
	info.CPUCount = 0
	info.Uptime = 0

	return nil
}
