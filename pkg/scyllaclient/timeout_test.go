// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestCustomTimeout(t *testing.T) {
	td := []struct {
		Name            string
		Context         context.Context
		Timeout         time.Duration
		ExpectedTimeout time.Duration
	}{
		{
			Name:            "timeout in context",
			Context:         scyllaclient.CustomTimeout(context.Background(), 5*time.Millisecond),
			Timeout:         0,
			ExpectedTimeout: 5 * time.Millisecond,
		},
		{
			Name:            "timeout in parameter",
			Context:         context.Background(),
			Timeout:         5 * time.Millisecond,
			ExpectedTimeout: 5 * time.Millisecond,
		},
		{
			Name:            "both timeouts, choose min",
			Context:         scyllaclient.CustomTimeout(context.Background(), 5*time.Millisecond),
			Timeout:         10 * time.Millisecond,
			ExpectedTimeout: 5 * time.Millisecond,
		},
		{
			Name:            "both timeouts, choose min",
			Context:         scyllaclient.CustomTimeout(context.Background(), 10*time.Millisecond),
			Timeout:         5 * time.Millisecond,
			ExpectedTimeout: 5 * time.Millisecond,
		},
	}

	for i := range td {
		test := td[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			done := make(chan struct{})

			hang := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-done
			})

			host, port, closeServer := scyllaclienttest.MakeServer(t, hang)
			defer closeServer()
			client := scyllaclienttest.MakeClient(t, host, port)

			_, err := client.Ping(test.Context, host, test.Timeout)
			close(done)

			golden := fmt.Sprintf("after %s: context deadline exceeded", test.ExpectedTimeout)
			if err == nil || err.Error() != golden {
				t.Fatalf("Ping() error = %v, expected %s", err, golden)
			}
		})
	}
}
