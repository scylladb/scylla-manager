// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/httpmw"
	"github.com/scylladb/mermaid/scyllaclient"
)

func newRouter(config config, rclone http.Handler, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	// Common middleware
	r.Use(
		httpmw.RequestLogger(logger),
	)
	// Common endpoints
	r.Get("/ping", httpmw.HeartbeatHandler())
	r.Get("/version", httpmw.VersionHandler())

	// Restricted access endpoints
	priv := r.With(
		httpmw.ValidateAuthToken(config.AuthToken, time.Second),
	)
	// Agent specific
	priv.Get("/agent/node_info", nodeInfo(net.JoinHostPort(config.Scylla.APIAddress, config.Scylla.APIPort)))
	// Rclone server
	priv.Mount("/agent/rclone", http.StripPrefix("/agent/rclone", rclone))
	// Scylla prometheus proxy
	priv.Mount("/metrics", promProxy(config))
	// Fallback to Scylla API proxy
	priv.NotFound(apiProxy(config))

	return r
}

func promProxy(config config) http.Handler {
	return &httputil.ReverseProxy{
		Director: director(net.JoinHostPort(config.Scylla.PrometheusAddress, config.Scylla.PrometheusPort)),
	}
}

func apiProxy(config config) http.HandlerFunc {
	h := &httputil.ReverseProxy{
		Director: director(net.JoinHostPort(config.Scylla.APIAddress, config.Scylla.APIPort)),
	}
	return h.ServeHTTP
}

func director(addr string) func(r *http.Request) {
	return func(r *http.Request) {
		r.Host = addr
		r.URL.Host = addr
		r.URL.Scheme = "http"
	}
}

func nodeInfo(addr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := scyllaclient.NewConfigClient(addr)

		nodeInfo, err := client.NodeInfo(r.Context())
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.Respond(w, r, errors.Wrap(err, "node info fetch"))
			return
		}

		render.Respond(w, r, nodeInfo)
	}
}
