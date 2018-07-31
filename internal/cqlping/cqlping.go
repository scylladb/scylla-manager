// Copyright (C) 2017 ScyllaDB

package cqlping

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/timeutc"
)

const port = "9042"

var options = []byte{4, 0, 0, 0, 5, 0, 0, 0, 0}

// Ping connects to a host on native port (9042), sends OPTIONS frame and
// expects SUPPORTED frame.
func Ping(ctx context.Context, host string) (time.Duration, error) {
	t := timeutc.Now()

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", host+":"+port)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if _, err := conn.Write(options); err != nil {
		return 0, err
	}
	var header [9]byte
	if _, err := conn.Read(header[:]); err != nil {
		return 0, err
	}
	if header[4] != 6 {
		return 0, errors.New("unexpected opt")
	}

	return timeutc.Since(t), nil
}
