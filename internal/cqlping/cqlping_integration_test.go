// Copyright (C) 2017 ScyllaDB

// +build all integration

package cqlping

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/mermaid/mermaidtest"
)

func TestPingIntegration(t *testing.T) {
	_, err := Ping(context.Background(), Config{
		Addr:    mermaidtest.ManagedClusterHosts[0] + ":9042",
		Timeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPingTLSIntegration(t *testing.T) {
	t.SkipNow()

	_, err := Ping(context.Background(), Config{
		Addr:    mermaidtest.ManagedClusterHosts[0] + ":9042",
		Timeout: 250 * time.Millisecond,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPingIntegrationTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		var buf [10]byte
		conn.Read(buf[:])
		time.Sleep(time.Second)
		conn.Write(buf[:])
	}()

	_, err = Ping(context.Background(), Config{
		Addr:    l.Addr().String(),
		Timeout: 250 * time.Millisecond,
	})
	if err == nil || !strings.HasSuffix(err.Error(), "i/o timeout") {
		t.Fatal("expected timeout error, got", err)
	}
}
