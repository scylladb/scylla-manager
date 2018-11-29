// Copyright (C) 2017 ScyllaDB

// +build all integration

package ssh_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/mermaid/internal/ssh"
	. "github.com/scylladb/mermaid/mermaidtest"
)

func TestPoolIntegration(t *testing.T) {
	var (
		network = "tcp"
		addr    = fmt.Sprint(ManagedClusterHosts[0], ":22")
		config  = ssh.NewDevelopmentClientConfig()
		pool    = ssh.NewPool(ssh.ContextDialer(ssh.DefaultDialer), time.Minute)
	)

	ctx := context.Background()

	t.Run("dial", func(t *testing.T) {
		Print("When: dial new server")
		c0, err := pool.DialContext(ctx, network, addr, config)
		if err != nil {
			t.Fatal(err)
		}

		Print("Then: client is cached")
		Print("And: ref count updated")
		if client, _, refCount := pool.Inspect(network, addr); client != c0 {
			t.Fatal("wrong client")
		} else if refCount != 1 {
			t.Fatal("wrong refCount")
		}

		Print("When: client is released")
		pool.Release(c0)

		Print("Then: ref count is updated")
		if _, _, refCount := pool.Inspect(network, addr); refCount != 0 {
			t.Fatal("wrong refCount")
		}

		Print("When: server is redialed")
		c1, err := pool.DialContext(ctx, network, addr, config)
		if err != nil {
			t.Fatal(err)
		}

		Print("Then: cached client is returned")
		if c0 != c1 {
			t.Fatal("wrong client")
		}

		pool.Release(c1)
	})

	t.Run("gc", func(t *testing.T) {
		Print("Given: there is a used connection")
		c0, err := pool.DialContext(ctx, network, addr, config)
		if err != nil {
			t.Fatal(err)
		}

		Print("And: connection idle time is exceeded")
		pool.SetLastUseTime(network, addr, time.Time{})

		Print("When: GC runs")
		pool.GC()

		Print("Then: connection is not closed")
		if client, _, _ := pool.Inspect(network, addr); client == nil {
			t.Fatal("GC failure")
		}

		Print("Given: there is unused connection")
		pool.Release(c0)

		Print("When: GC runs")
		pool.GC()

		Print("Then: connection is closed")
		if client, _, _ := pool.Inspect(network, addr); client != nil {
			t.Fatal("GC failure")
		}
	})

	t.Run("close", func(t *testing.T) {
		Print("Given: there is a used connection")
		_, err := pool.DialContext(ctx, network, addr, config)
		if err != nil {
			t.Fatal(err)
		}

		Print("When: pool is closed")
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}

		Print("Then connection is closed")
		if client, _, _ := pool.Inspect(network, addr); client != nil {
			t.Fatal("Close failure")
		}
	})
}
