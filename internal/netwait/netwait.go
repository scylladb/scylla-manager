// Copyright (C) 2017 ScyllaDB

package netwait

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"go.uber.org/multierr"
)

// AnyAddr tries to open a TCP connection to any of the provided addresses,
// returns first address it could connect to.
func AnyAddr(ctx context.Context, addr ...string) (string, error) {
	return DefaultWaiter.WaitAnyAddr(ctx, addr...)
}

// AnyHostPort tries to connect to any of the hosts, returns first host if
// could connect to over the given port.
func AnyHostPort(ctx context.Context, hosts []string, port string) (string, error) {
	return DefaultWaiter.WaitAnyHostPort(ctx, hosts, port)
}

// Waiter specifies waiting parameters.
type Waiter struct {
	RetryBackoff time.Duration
	MaxAttempts  int
	Logger       log.Logger
}

var DefaultWaiter = Waiter{
	RetryBackoff: 2 * time.Second,
	MaxAttempts:  60,
}

// WaitAnyAddr tries to open a TCP connection to any of the provided addresses,
// returns first address it could connect to.
func (w Waiter) WaitAnyAddr(ctx context.Context, addr ...string) (string, error) {
	t := time.NewTimer(w.RetryBackoff)
	defer t.Stop()

	var (
		a   string
		err error
	)
	for i := 0; i < w.MaxAttempts; i++ {
		t.Reset(w.RetryBackoff)

		a, err = tryConnectToAny(addr)

		if err != nil {
			w.Logger.Info(ctx, "Waiting for network connection",
				"sleep", w.RetryBackoff,
				"error", err,
			)
			select {
			case <-t.C:
				continue
			case <-ctx.Done():
				return "", err
			}
		}

		return a, nil
	}

	return "", errors.Wrapf(err, "giving up after %d attempts", w.MaxAttempts)
}

// tryConnectToAny tries to open a TCP connection to any of the provided
// addresses, attempts are sequential, it returns first successful address or
// error if failed to connect to any address.
func tryConnectToAny(addrs []string) (string, error) {
	var errs error

	for _, addr := range addrs {
		conn, err := net.Dial("tcp", addr)
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			return addr, nil
		}
		errs = multierr.Append(errs, err)
	}

	return "", errs
}

// WaitAnyHostPort tries to connect to any of the hosts, returns first host if
// could connect to over the given port.
func (w Waiter) WaitAnyHostPort(ctx context.Context, hosts []string, port string) (string, error) {
	addr, err := w.WaitAnyAddr(ctx, joinHostsPort(hosts, port)...)
	if err != nil {
		return addr, err
	}

	// Extract host from address before returning
	host, _, _ := net.SplitHostPort(addr)
	return host, nil
}

func joinHostsPort(hosts []string, port string) []string {
	out := make([]string, len(hosts))
	for i, h := range hosts {
		out[i] = net.JoinHostPort(h, port)
	}
	return out
}
