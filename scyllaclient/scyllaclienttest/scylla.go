// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"io"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/scylladb/mermaid/scyllaclient"
)

func NewFakeScyllaServer(t *testing.T, file string) (*scyllaclient.Client, func()) {
	return NewFakeScyllaServerMatching(t, FileMatcher(file))
}

func NewFakeScyllaServerMatching(t *testing.T, m Matcher) (*scyllaclient.Client, func()) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error("ParseForm() error", err)
		}

		// Emulate ScyllaDB bug
		r.Header.Set("Content-Type", "text/plain")

		file := m(r)

		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.Copy(w, f); err != nil {
			t.Error("Copy() error", err)
		}
	})

	host, port, close := server(t, h)
	return client(t, host, port), close
}

func NewFakeScyllaV2Server(t *testing.T, file string) (*scyllaclient.ConfigClient, func()) {
	return NewFakeScyllaV2ServerMatching(t, FileMatcher(file))
}

func NewFakeScyllaV2ServerMatching(t *testing.T, m Matcher) (*scyllaclient.ConfigClient, func()) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		file := m(r)
		w.Header().Set("Content-Type", "application/json")

		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.Copy(w, f); err != nil {
			t.Error("Copy() error", err)
		}
	})

	host, port, close := server(t, h)
	return scyllaclient.NewConfigClient(net.JoinHostPort(host, port)), close
}
