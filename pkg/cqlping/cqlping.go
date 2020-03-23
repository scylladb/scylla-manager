// Copyright (C) 2017 ScyllaDB

package cqlping

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

var (
	// ErrTimeout is returned when CQL ping times out.
	ErrTimeout = errors.New("timeout")

	// ErrUnauthorised is returned when CQL
	ErrUnauthorised = errors.New("unauthorised")
)

// Config specifies the ping configuration, note that timeout is mandatory and
// zero timeout will result in errors.
type Config struct {
	Addr      string
	Username  string
	Password  string
	Timeout   time.Duration
	TLSConfig *tls.Config
}

// Ping checks if host is available, it returns RTT and error. Special errors
// are ErrTimeout and ErrUnauthorised. If no credentials are specified ping
// is based on writing a CQL OPTIONS frame to server connection. If credentials
// are specified ping is based on executing "SELECT now() FROM system.local"
// query.
func Ping(ctx context.Context, config Config) (time.Duration, error) {
	if config.Username == "" {
		return simplePing(ctx, config)
	}
	return queryPing(ctx, config)
}

// options is wire encoded CQL OPTIONS frame.
var options = []byte{4, 0, 0, 0, 5, 0, 0, 0, 0}

// simplePing connects to a host on native port, sends OPTIONS frame and waits
// for SUPPORTED frame in response. If connection fails, operation timeouts or
// receives unexpected payload an error is returned.
func simplePing(ctx context.Context, config Config) (rtt time.Duration, err error) {
	t := timeutc.Now()
	defer func() {
		rtt = timeutc.Since(t)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			err = ErrTimeout
		}
	}()

	var (
		conn   net.Conn
		header [9]byte
	)

	d := &net.Dialer{
		Deadline: t.Add(config.Timeout),
	}
	if config.TLSConfig != nil {
		conn, err = tls.DialWithDialer(d, "tcp", config.Addr, config.TLSConfig)
	} else {
		conn, err = d.DialContext(ctx, "tcp", config.Addr)
	}
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(d.Deadline); err != nil {
		return 0, err
	}

	if _, err = conn.Write(options); err != nil {
		return 0, err
	}
	if _, err = conn.Read(header[:]); err != nil {
		return 0, err
	}
	if header[4] != 6 {
		return 0, errors.New("unexpected opt")
	}

	return 0, nil
}

const cqlUnauthorisedMessage = "Username and/or password are incorrect"

func queryPing(ctx context.Context, config Config) (rtt time.Duration, err error) {
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
		Username: config.Username,
		Password: config.Password,
	}

	// Avoid openning too much connections
	cluster.NumConns = 1
	cluster.DisableInitialHostLookup = true
	cluster.ReconnectInterval = 0
	// Do not use TokenAwareHostPolicy as it ignores NumConns
	cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()

	// Set timeout
	cluster.ConnectTimeout = config.Timeout

	t := timeutc.Now()
	defer func() {
		rtt = timeutc.Since(t)
		if err != nil {
			if rtt >= config.Timeout {
				err = ErrTimeout
			} else if strings.HasSuffix(err.Error(), cqlUnauthorisedMessage) {
				err = ErrUnauthorised
			}
		}
	}()

	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		return 0, errors.Wrap(err, "create session")
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(ctx, config.Timeout-timeutc.Since(t))
	defer cancel()

	const stmt = "SELECT now() FROM system.local"
	q := session.Query(stmt).WithContext(ctx).Consistency(gocql.One)

	var date []byte
	if err := q.Scan(&date); err != nil {
		return 0, errors.Wrap(err, "scan")
	}

	return 0, nil
}
