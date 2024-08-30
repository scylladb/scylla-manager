// Copyright (C) 2017 ScyllaDB

package cqlping

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util"

	"github.com/scylladb/scylla-manager/v3/pkg/ping"
)

func TestPingTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	done := make(chan struct{})
	defer close(done)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		select {
		case <-time.After(time.Second):
			break
		case <-done:
			break
		}
		conn.Close()
	}()

	config := Config{
		Addr:    l.Addr().String(),
		Timeout: 250 * time.Millisecond,
	}

	t.Run("simple", func(t *testing.T) {
		d, err := NativeCQLPing(context.Background(), config, log.NopLogger)
		if err != ping.ErrTimeout {
			t.Errorf("NativeCQLPing() error %s, expected timeout", err)
		}
		if a, b := util.EpsilonRange(config.Timeout); d < a || d > b {
			t.Errorf("NativeCQLPing() not within expected time margin %v got %v", config.Timeout, d)
		}
	})

	t.Run("query", func(t *testing.T) {
		d, err := QueryPing(context.Background(), config, "", "")
		if err != ping.ErrTimeout {
			t.Errorf("QueryPing() error %s, expected timeout", err)
		}
		if a, b := util.EpsilonRange(config.Timeout); d < a || d > b {
			t.Errorf("QueryPing() not within expected time margin %v got %v", config.Timeout, d)
		}
	})
}
