// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"net"
	"net/http"
	"testing"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func NewFakeScyllaServer(t *testing.T, file string) (client *scyllaclient.Client, closeServer func()) {
	return NewFakeScyllaServerMatching(t, FileMatcher(file))
}

func NewFakeScyllaServerMatching(t *testing.T, m Matcher) (client *scyllaclient.Client, closeServer func()) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error("ParseForm() error", err)
		}
		// Emulate ScyllaDB bug
		r.Header.Set("Content-Type", "text/plain")
		sendFile(t, w, m(r))
	})

	host, port, closeServer := makeServer(t, h)
	client = makeClient(t, host, port)
	return
}

func NewFakeScyllaV2Server(t *testing.T, file string) (client *scyllaclient.ConfigClient, closeServer func()) {
	return NewFakeScyllaV2ServerMatching(t, FileMatcher(file))
}

func NewFakeScyllaV2ServerMatching(t *testing.T, m Matcher) (client *scyllaclient.ConfigClient, closeServer func()) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		sendFile(t, w, m(r))
	})

	host, port, closeServer := makeServer(t, h)
	client = scyllaclient.NewConfigClient(net.JoinHostPort(host, port))
	return
}
