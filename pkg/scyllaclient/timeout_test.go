// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/swagger/gen/agent/models"
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

func TestTimeoutWhileStreaming(t *testing.T) {
	s := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"list":[`)
		b, err := json.Marshal(models.ListItem{
			IsDir:   false,
			ModTime: strfmt.DateTime{},
			Name:    "foo",
			Path:    "/bar/foo",
			Size:    42,
		})
		if err != nil {
			panic(err)
		}
		var (
			d = 150 * time.Millisecond
			s = 10 * time.Millisecond
		)
		for i := 0; i < int(d/s); i++ {
			w.Write(b)
			w.Write([]byte(","))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(s)
		}
		w.Write(b)
		w.Write([]byte("]}"))
	})

	host, port, closeServer := scyllaclienttest.MakeServer(t, s)
	defer closeServer()
	client := scyllaclienttest.MakeClient(t, host, port, func(config *scyllaclient.Config) {
		config.Timeout = 50 * time.Millisecond
	})

	l, err := client.RcloneListDir(context.Background(), host, "s3:foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got %d items", len(l))
}
