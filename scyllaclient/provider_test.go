// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/mermaid/uuid"
)

type mockProvider struct {
	client *Client
	err    error
	called bool
}

func (m *mockProvider) Client(ctx context.Context, clusterID uuid.UUID) (*Client, error) {
	m.called = true
	return m.client, m.err
}

func TestCachedProvider(t *testing.T) {
	t.Parallel()

	id := uuid.MustRandom()
	m := mockProvider{}
	p := NewCachedProvider(m.Client)

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
	m.client = &Client{}
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
