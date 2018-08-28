// Copyright (C) 2017 ScyllaDB

// +build all integration

package cqlping

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/mermaid/mermaidtest"
)

func TestPingIntegration(t *testing.T) {
	_, err := Ping(context.Background(), time.Second, mermaidtest.ManagedClusterHosts[0])
	if err != nil {
		t.Fatal(err)
	}
}

func TestPingIntegrationTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:"+port)
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

	_, err = Ping(context.Background(), 100*time.Millisecond, "127.0.0.1")
	if err == nil || !strings.HasSuffix(err.Error(), "i/o timeout") {
		t.Fatal("expected timeout error, got", err)
	}
}
