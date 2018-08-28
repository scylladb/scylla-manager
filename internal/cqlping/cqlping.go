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
func Ping(ctx context.Context, timeout time.Duration, host string) (time.Duration, error) {
	t := timeutc.Now()

	d := net.Dialer{
		Deadline: t.Add(timeout),
	}

	var (
		err    error
		header [9]byte
	)

	conn, err := d.DialContext(ctx, "tcp", host+":"+port)
	if err != nil {
		goto exit
	}
	defer conn.Close()
	if err = conn.SetDeadline(d.Deadline); err != nil {
		goto exit
	}

	if _, err = conn.Write(options); err != nil {
		goto exit
	}
	if _, err = conn.Read(header[:]); err != nil {
		goto exit
	}
	if header[4] != 6 {
		err = errors.New("unexpected opt")
	}

exit:
	return timeutc.Since(t), err
}
