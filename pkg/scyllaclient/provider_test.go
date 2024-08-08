// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type mockProvider struct {
	client *scyllaclient.Client
	err    error
	called bool
}

func (m *mockProvider) Client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	m.called = true
	return m.client, m.err
}

func TestCachedProvider(t *testing.T) {
	t.Parallel()

	id := uuid.MustRandom()
	m := mockProvider{}
	p := scyllaclient.NewCachedProvider(m.Client, server.DefaultConfig().ClientCacheTimeout, log.Logger{})

	// Error
	m.err = errors.New("mock")

	c, err := p.Client(context.Background(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if err != m.err {
		t.Fatal(err)
	}

	// Success
	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/host_id_map_localhost.json")
	defer closeServer()

	m.client = client
	m.err = nil
	m.called = false

	c, err = p.Client(context.Background(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if err != m.err {
		t.Fatal(err)
	}

	// Cached
	m.called = false

	c, err = p.Client(context.Background(), id)
	if m.called {
		t.Fatal("called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if err != m.err {
		t.Fatal(err)
	}

	// Cached but changed
	m.called = false
	m.client.Config().Hosts[0] = "" // make hosts change without starting new server
	time.Sleep(15 * time.Second)    // cache checks for changed hosts every 15s
	c, err = p.Client(context.Background(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if err != m.err {
		t.Fatal(err)
	}

	// Invalidate
	p.Invalidate(id)

	m.called = false

	c, err = p.Client(context.Background(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if err != m.err {
		t.Fatal(err)
	}
}
