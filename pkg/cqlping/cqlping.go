// Copyright (C) 2017 ScyllaDB

package cqlping

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

var (
	// ErrTimeout is returned when CQL ping times out.
	ErrTimeout = errors.New("timeout")
)

// options is wire encoded CQL OPTIONS frame.
var options = []byte{4, 0, 0, 0, 5, 0, 0, 0, 0}

// Config specifies the ping configuration, note that timeout is mandatory and
// zero timeout will result in errors.
type Config struct {
	Addr      string
	Timeout   time.Duration
	TLSConfig *tls.Config
}

// Ping connects to a host on native port, sends OPTIONS frame and waits for
// SUPPORTED frame in response. If connection fails, operation timeouts or
// receives unexpected payload an error is returned.
func Ping(ctx context.Context, config Config) (time.Duration, error) {
	t := timeutc.Now()

	d := &net.Dialer{
		Deadline: t.Add(config.Timeout),
	}

	var (
		conn   net.Conn
		err    error
		header [9]byte
	)

	if config.TLSConfig != nil {
		conn, err = tls.DialWithDialer(d, "tcp", config.Addr, config.TLSConfig)
	} else {
		conn, err = d.DialContext(ctx, "tcp", config.Addr)
	}

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
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return timeutc.Since(t), ErrTimeout
		}
	}

	return timeutc.Since(t), err
}
