// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"net/http"
	"strings"
	"testing"

	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/scylladb/mermaid/scyllaclient"
)

func NewFakeRcloneServer(t *testing.T) (*scyllaclient.Client, func()) {
	rc := rcserver.New()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/rclone")
		rc.ServeHTTP(w, r)
	})

	host, port, close := server(t, h)
	return client(t, host, port), close
}
