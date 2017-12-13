// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"net"

	"github.com/scylladb/mermaid/mermaidclient/internal/models"
)

// Cluster is cluster.Cluster representation.
type Cluster = models.Cluster

// RepairProgressRow contains shard progress info.
type RepairProgressRow struct {
	Host     net.IP
	Shard    int
	Progress int
	Error    int
	Empty    bool
}

// RepairUnit is repair.Unit representation.
type RepairUnit = models.RepairUnit

// Task is a sched.Task representation.
type Task = models.Task

// Schedule is a sched.Schedule representation.
type Schedule = models.Schedule
