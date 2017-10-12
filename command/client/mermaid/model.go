// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"net"

	"github.com/scylladb/mermaid/command/client/mermaid/internal/models"
)

type RepairProgressRow struct {
	Host     net.IP
	Shard    int
	Progress int
	Error    int
}

type RepairUnit = models.RepairUnit
