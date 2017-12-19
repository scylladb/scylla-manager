// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"net"
	"time"
)

var (
	// DefaultDialTimeout is the timeout used in DefaultDialer.
	DefaultDialTimeout = 10 * time.Second

	// DefaultRPCTimeout is the timeout that should be used for RPC calls with scylla.
	DefaultRPCTimeout = DefaultDialTimeout + 20*time.Second
)

// DefaultDialer specifies default dial options.
var DefaultDialer = net.Dialer{
	Timeout:   DefaultDialTimeout,
	KeepAlive: 30 * time.Second,
	DualStack: true,
}
