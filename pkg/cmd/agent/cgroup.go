// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"
	"strconv"
)

const memoryLimitFile = "/sys/fs/cgroup/memory/scylla.slice/scylla-helper.slice/memory.limit_in_bytes"

func cgroupMemoryLimit() (int64, error) {
	buf, err := os.ReadFile(memoryLimitFile)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(buf), 10, 64)
}
