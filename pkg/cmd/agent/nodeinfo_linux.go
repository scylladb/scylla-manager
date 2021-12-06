// Copyright (C) 2017 ScyllaDB
//go:build linux
// +build linux

package main

import (
	"runtime"

	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"golang.org/x/sys/unix"
)

func (h *nodeInfoHandler) sysInfo(info *scyllaclient.NodeInfo) error {
	si := unix.Sysinfo_t{}
	if err := unix.Sysinfo(&si); err != nil {
		return err
	}

	info.MemoryTotal = int64(si.Totalram)
	info.CPUCount = int64(runtime.NumCPU())
	info.Uptime = si.Uptime

	return nil
}
