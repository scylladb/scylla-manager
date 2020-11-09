// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-chi/chi"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/auth"
	"github.com/scylladb/scylla-manager/pkg/util/httphandler"
	"github.com/scylladb/scylla-manager/pkg/util/httplog"
)

var unauthorizedErrorBody = json.RawMessage(`{"message":"unauthorized","code":401}`)

func newRouter(config config, rclone http.Handler, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	// Common middleware
	r.Use(
		httplog.RequestLogger(logger),
	)
	// Common endpoints
	r.Get("/ping", httphandler.Heartbeat())
	r.Get("/version", httphandler.Version())

	// Restricted access endpoints
	priv := r.With(
		auth.ValidateToken(config.AuthToken, time.Second, unauthorizedErrorBody),
	)
	// Agent specific endpoints
	priv.Mount("/agent", newAgentHandler(config, rclone))
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
