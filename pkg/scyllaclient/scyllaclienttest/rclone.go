// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/scylladb/mermaid/pkg/rclone/rcserver"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func NewFakeRcloneServer(t *testing.T, matchers ...Matcher) (client *scyllaclient.Client, closeServer func()) {
	rc := rcserver.New()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, m := range matchers {
			if m(r) != "" {
				SendFile(t, w, m(r))
				return
			}
		}

		if p := path.Clean(r.URL.Path) + "/"; strings.HasPrefix(p, "/agent/rclone/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/agent/rclone")
			rc.ServeHTTP(w, r)
		} else {
			t.Error("No matcher for path", r.URL.Path)
		}
	})

	host, port, closeServer := MakeServer(t, h)
	client = MakeClient(t, host, port)
	return
}
