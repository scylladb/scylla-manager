// Copyright (C) 2017 ScyllaDB

package netwait

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
)

func TestWaiterWaitAnyAddr(t *testing.T) {
	table := []struct {
		Name  string
		Addrs []string
		Error bool
	}{
		{
			Name:  "No hosts",
			Addrs: nil,
		},
		{
			Name:  "Single host",
			Addrs: []string{"127.0.0.1:10000"},
		},
		{
			Name:  "Multiple hosts",
			Addrs: []string{"127.0.0.1:10000", "127.0.0.1:10001"},
		},
		{
			Name:  "Multiple hosts one inaccessible",
			Addrs: []string{"127.0.0.1:10005", "127.0.0.1:10000", "127.0.0.1:10001"},
		},
		{
			Name:  "Multiple hosts only one accessible",
			Addrs: []string{"127.0.0.1:10005", "127.0.0.1:10006", "127.0.0.1:10001"},
		},
		{
			Name:  "Multiple inaccessible hosts",
			Addrs: []string{"127.0.0.1:10005", "127.0.0.1:10006", "127.0.0.1:10007"},
			Error: true,
		},
		{
			Name:  "Single inaccessible host",
			Addrs: []string{"127.0.0.1:10005", "127.0.0.1:10006", "127.0.0.1:10007"},
			Error: true,
		},
	}

	l, err := net.Listen("tcp", "127.0.0.1:10000")
	if err != nil {
		t.Skip("Failed to start test server at port 10000", err)
	}
	defer l.Close()

	l2, err := net.Listen("tcp", "127.0.0.1:10001")
	if err != nil {
		t.Skip("Failed to start test server at port 10001", err)
	}
	defer l2.Close()

	w := Waiter{
		RetryBackoff: 10 * time.Millisecond,
		MaxAttempts:  100,
		Logger:       log.NewDevelopment(),
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := w.WaitAnyAddr(context.Background(), test.Addrs...)
			if test.Error && err == nil {
				t.Fatal("Expected error")
			}
			if !test.Error && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWaiterWaitAnyAddrRetry(t *testing.T) {
	done := make(chan error)

	w := Waiter{
		RetryBackoff: 10 * time.Millisecond,
		MaxAttempts:  100,
		Logger:       log.NewDevelopment(),
	}

	go func() {
		_, err := w.WaitAnyAddr(context.Background(), "127.0.0.1:10000")
		done <- err
	}()

	time.Sleep(5 * w.RetryBackoff)

	l, err := net.Listen("tcp", "127.0.0.1:10000")
	if err != nil {
		t.Skip("Failed to start test server at port 10000", err)
	}
	defer l.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * w.RetryBackoff):
		t.Fatal("Retry timeout")
	}
}

func TestJoinHostPort(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name   string
		Hosts  []string
		Port   string
		Golden []string
	}{
		{
			Name:   "No hosts",
			Hosts:  nil,
			Golden: nil,
		},
		{
			Name:   "Single host",
			Hosts:  []string{"127.0.0.1"},
			Port:   "10000",
			Golden: []string{"127.0.0.1:10000"},
		},
		{
			Name:   "Multiple hosts",
			Port:   "10000",
			Hosts:  []string{"127.0.0.1", "127.0.0.2"},
			Golden: []string{"127.0.0.1:10000", "127.0.0.2:10000"},
		},
	}
	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			res := joinHostsPort(test.Hosts, test.Port)
			if diff := cmp.Diff(test.Golden, res); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
