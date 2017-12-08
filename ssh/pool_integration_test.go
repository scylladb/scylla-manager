// Copyright (C) 2017 ScyllaDB

// +build all integration

package ssh

import (
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/mermaid/mermaidtest"
)

func TestPoolIntegration(t *testing.T) {
	var (
		host   = mermaidtest.ManagedClusterHosts[0]
		config = NewDevelopmentClientConfig()
		pool   = NewPool(DefaultDialer, time.Minute)
	)

	inspect := func() *poolConn {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return pool.conns[key("tcp", fmt.Sprint(host, ":22"))]
	}

	t.Run("dial", func(t *testing.T) {
		// When dial new server
		c0, err := pool.Dial("tcp", fmt.Sprint(host, ":22"), config)
		if err != nil {
			t.Fatal(err)
		}
		// Then client is cached
		// And ref count updated
		if inspect().client != c0 {
			t.Fatal("wrong client")
		}
		if inspect().refCount != 1 {
			t.Fatal("wrong refCount")
		}

		// When client is released
		pool.Release(c0)
		// Then ref count is updated
		if inspect().refCount != 0 {
			t.Fatal("wrong refCount")
		}

		// When server is redialed
		c1, err := pool.Dial("tcp", fmt.Sprint(host, ":22"), config)
		if err != nil {
			t.Fatal(err)
		}
		// Then a cached client is returned
		if c0 != c1 {
			t.Fatal("wrong client")
		}

		pool.Release(c1)
	})

	t.Run("gc", func(t *testing.T) {
		// Given there is a used connection
		c0, err := pool.Dial("tcp", fmt.Sprint(host, ":22"), config)
		if err != nil {
			t.Fatal(err)
		}
		pc := inspect()
		pc.lastUse = pc.lastUse.Add(-time.Hour)
		// When GC
		pool.GC()
		// Then connection shall not be closed
		if inspect() == nil {
			t.Fatal("GC failure")
		}

		// Given there is a NOT used connection
		pool.Release(c0)
		// When GC
		pool.GC()
		// Then connection is closed
		if inspect() != nil {
			t.Fatal("GC failure")
		}
	})

	t.Run("close", func(t *testing.T) {
		// Given there is a used connection
		_, err := pool.Dial("tcp", fmt.Sprint(host, ":22"), config)
		if err != nil {
			t.Fatal(err)
		}
		// When pool is closed
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
		// Then connection is closed
		if inspect() != nil {
			t.Fatal("Close failure")
		}
	})
}
