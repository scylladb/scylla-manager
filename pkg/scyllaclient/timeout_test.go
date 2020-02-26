// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
)

func TestCustomTimeout(t *testing.T) {
	done := make(chan struct{})

	hang := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
	})

	host, port, closeServer := scyllaclienttest.MakeServer(t, hang)
	defer closeServer()
	client := scyllaclienttest.MakeClient(t, host, port)

	ctx := scyllaclient.CustomTimeout(context.Background(), 5*time.Millisecond)
	_, err := client.Ping(ctx, host)
	close(done)

	const golden = "timeout after 5ms"
	if err == nil || err.Error() != golden {
		t.Fatalf("Ping() error = %v, expected %s", err, golden)
	}
}
