// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/http"
	"net/http/httputil"
	"path"
	"strings"
)

type router struct {
	config config
	rclone http.Handler
	proxy  *httputil.ReverseProxy
}

func newRouter(config config, rclone http.Handler) *router {
	return &router{
		config: config,
		rclone: rclone,
		proxy: &httputil.ReverseProxy{
			Director: director(config.Scylla),
		},
	}
}

func director(c scyllaConfig) func(r *http.Request) {
	var (
		promAddr = net.JoinHostPort(c.PrometheusAddress, c.PrometheusPort)
		apiAddr  = net.JoinHostPort(c.APIAddress, c.APIPort)
	)

	return func(r *http.Request) {
		var addr string
		if strings.HasPrefix(r.URL.Path, "/metrics") {
			addr = promAddr
		} else {
			addr = apiAddr
		}

		r.Host = addr
		r.URL.Host = addr
		r.URL.Scheme = "http"
	}
}

func (mux *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Path) + "/"
	switch {
	case strings.HasPrefix(p, "/agent/rclone/"):
		// Stripping prefix to use clean paths in rclone server
		// eg. "/agent/rclone/operations/about" to "/operations/about".
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/agent/rclone")
		mux.rclone.ServeHTTP(w, r)
	case strings.HasPrefix(p, "/agent/node_info"):
		mux.getNodeInfo(w, r)
	default:
		mux.proxy.ServeHTTP(w, r)
	}
}
