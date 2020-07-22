// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
)

func TestClientPing(t *testing.T) {
	t.Parallel()

	c, close := scyllaclienttest.NewFakeScyllaServer(t, "/dev/null")
	defer close()

	var timeout time.Duration

	if _, err := c.Ping(context.Background(), scyllaclienttest.TestHost, timeout); err != nil {
		t.Fatal(err)
	}

	_, err := c.Ping(context.Background(), "localhost:0", timeout)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPickNRandomHosts(t *testing.T) {
	table := []struct {
		H []string
		N int
		E int
	}{
		{
			H: []string{"a"},
			N: 1,
			E: 1,
		},
		{
			H: []string{"a"},
			N: 4,
			E: 1,
		},
		{
			H: []string{"a", "a"},
			N: 2,
			E: 2,
		},
		{
			H: []string{"a", "b", "c"},
			N: 2,
			E: 2,
		},
	}

	for i, test := range table {
		picked := scyllaclient.PickNRandomHosts(test.N, test.H)
		if len(picked) != test.E {
			t.Errorf("Picked %d hosts, expected %d in test %d", len(picked), test.E, i)
		}
	}
}
