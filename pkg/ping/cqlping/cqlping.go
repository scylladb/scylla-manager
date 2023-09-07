// Copyright (C) 2017 ScyllaDB

package cqlping

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// Config specifies the ping configuration, note that timeout is mandatory and
// zero timeout will result in errors.
type Config struct {
	Addr      string
	Timeout   time.Duration
	TLSConfig *tls.Config
}

// options is wire encoded CQL OPTIONS frame.
var options = []byte{4, 0, 0, 0, 5, 0, 0, 0, 0}

// NativeCQLPing connects to a host on native port, sends OPTIONS frame and waits
// for SUPPORTED frame in response. If connection fails, operation timeouts or
// receives unexpected payload an error is returned.
// It returns rtt and error.
func NativeCQLPing(ctx context.Context, config Config, logger log.Logger) (rtt time.Duration, err error) {
	backoff := retry.NewExponentialBackoff(time.Millisecond, config.Timeout, config.Timeout/3, 2, 0)
	deadline := timeutc.Now().Add(config.Timeout)
	t := timeutc.Now()
	for attempt := 1; ; attempt++ {
		err = nativeCQLPingOnce(ctx, config, deadline)
		if err == nil {
			return timeutc.Since(t), nil
		}
		duration := backoff.NextBackOff()
		if duration == retry.Stop {
			return timeutc.Since(t), ping.ErrTimeout
		}
		logger.Debug(ctx, fmt.Sprintf("CQL ping attempt %d failed", attempt), "error", err)
		time.Sleep(duration)
	}
}

func nativeCQLPingOnce(ctx context.Context, config Config, deadline time.Time) (err error) {
	var (
		conn   net.Conn
		header [9]byte
	)

	d := &net.Dialer{
		Deadline: deadline,
	}
	network := "tcp"
	if strings.Count(config.Addr, ":") > 1 {
		network = "tcp6"
	}
	if config.TLSConfig != nil {
		conn, err = tls.DialWithDialer(d, network, config.Addr, config.TLSConfig)
	} else {
		conn, err = d.DialContext(ctx, network, config.Addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(d.Deadline); err != nil {
		return err
	}

	if _, err = conn.Write(options); err != nil {
		return err
	}
	if _, err = conn.Read(header[:]); err != nil {
		return err
	}
	if header[4] != 6 {
		return errors.New("unexpected opt")
	}

	return nil
}

var cqlUnauthorisedMessage = []string{
	"authentication failed",
	"Username and/or password are incorrect",
}

// QueryPing executes "SELECT now() FROM system.local" on a single host.
// It returns rtt and error.
func QueryPing(_ context.Context, config Config, username, password string) (rtt time.Duration, err error) {
	host, port, err := net.SplitHostPort(config.Addr)
	if err != nil {
		return 0, errors.Wrap(err, "split host port")
	}

	cluster := gocql.NewCluster(host)
	// Set port if needed
	if port != "9042" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return 0, err
		}
		cluster.Port = p
	}
	// Set TLS config if needed
	if config.TLSConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{
			Config: config.TLSConfig.Clone(),
		}
	}
	// Set credentials
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}

	// Skip protocol discovery. We use the protocol 3 to indicate the connection
	// is for health-check purpose only and should not be included in the
	// cluster metrics.
	cluster.ProtoVersion = 3
	cluster.ReconnectInterval = 0

	// Set timeout
	cluster.ConnectTimeout = config.Timeout
	cluster.Timeout = config.Timeout

	// Disable all events
	cluster.Events.DisableNodeStatusEvents = true
	cluster.Events.DisableTopologyEvents = true
	cluster.Events.DisableSchemaEvents = true

	// Disable write coalescing
	cluster.WriteCoalesceWaitTime = 0

	t := timeutc.Now()
	defer func() {
		rtt = timeutc.Since(t)
		if err != nil {
			if rtt >= config.Timeout {
				err = ping.ErrTimeout
			} else {
				for _, m := range cqlUnauthorisedMessage {
					if strings.HasSuffix(err.Error(), m) {
						err = ping.ErrUnauthorised
						break
					}
				}
			}
		}
	}()

	e, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		return 0, err
	}
	defer e.Close()

	var date []byte
	iter := e.Iter("SELECT now() FROM system.local")
	iter.Scan(&date)
	return 0, iter.Close()
}
