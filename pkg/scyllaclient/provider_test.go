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

func newCachedProvider(t *testing.T, f scyllaclient.ProviderFunc, validity, hostsValidity time.Duration) *scyllaclient.CachedProvider {
	t.Helper()
	p, err := scyllaclient.NewCachedProvider(f, validity, hostsValidity, log.Logger{})
	if err != nil {
		t.Fatalf("NewCachedProvider() error: %s", err)
	}
	return p
}

func TestCachedProvider(t *testing.T) {
	t.Parallel()

	id := uuid.MustRandom()
	m := mockProvider{}
	// Short hosts validity makes checking for changed hosts quick to test
	const hostsValidity = 100 * time.Millisecond
	p := newCachedProvider(t, m.Client, server.DefaultConfig().ClientCacheTimeout, hostsValidity)

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
	time.Sleep(2 * hostsValidity)   // cache checks for changed hosts every hostsValidity
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

func TestNewCachedProviderValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		validity      time.Duration
		hostsValidity time.Duration
		err           bool
	}{
		{
			name:          "valid",
			validity:      time.Minute,
			hostsValidity: time.Second,
		},
		{
			name:          "zero validity disables caching",
			validity:      0,
			hostsValidity: time.Second,
		},
		{
			name:          "negative validity",
			validity:      -time.Minute,
			hostsValidity: time.Second,
			err:           true,
		},
		{
			name:          "zero hosts validity checks hosts on every call",
			validity:      time.Minute,
			hostsValidity: 0,
		},
		{
			name:          "negative hosts validity",
			validity:      time.Minute,
			hostsValidity: -time.Second,
			err:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, err := scyllaclient.NewCachedProvider(new(mockProvider).Client, tc.validity, tc.hostsValidity, log.Logger{})
			if tc.err {
				if err == nil {
					t.Fatal("NewCachedProvider() expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewCachedProvider() error: %s", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close() error: %s", err)
			}
		})
	}
}
