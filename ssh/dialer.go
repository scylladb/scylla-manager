// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"net"
	"time"
)

// DefaultDialer specifies default dial options and is used by DefaultPool.
var DefaultDialer = net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
	DualStack: true,
}
