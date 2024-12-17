// Copyright (C) 2017 ScyllaDB
//go:build linux
// +build linux

package main

import (
	"runtime"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"golang.org/x/sys/unix"
)

func (h *nodeInfoHandler) sysInfo(info *scyllaclient.NodeInfo) error {
	si := unix.Sysinfo_t{}
	if err := unix.Sysinfo(&si); err != nil {
		return err
	}

	var statfs unix.Statfs_t
	if err := unix.Statfs(info.DataDirectory, &statfs); err != nil {
		return errors.Wrap(err, "statfs")
	}
	total := statfs.Blocks * uint64(statfs.Bsize)

	info.MemoryTotal = int64(si.Totalram)
	info.CPUCount = int64(runtime.NumCPU())
	info.Uptime = si.Uptime
	info.StorageSize = total

	return nil
}
