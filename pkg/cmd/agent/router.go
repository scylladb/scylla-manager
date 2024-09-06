// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/auth"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
	"github.com/scylladb/scylla-manager/v3/pkg/restapi"
)

var unauthorizedErrorBody = json.RawMessage(`{"message":"unauthorized","code":401}`)

func newRouter(c agent.Config, metrics AgentMetrics, rclone http.Handler, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	// Common middleware
	r.Use(
		RequestLogger(logger, metrics),
	)
	// Common endpoints
	r.Get("/ping", restapi.Heartbeat())
	r.Get("/version", restapi.Version())

	// Restricted access endpoints
	priv := r.With(
		auth.ValidateToken(c.AuthToken, time.Second, unauthorizedErrorBody),
	)
	// Agent specific endpoints
	priv.Mount("/agent", newAgentHandler(c, rclone))
	// Scylla prometheus proxy
	priv.Mount("/metrics", promProxy(c))
	// Fallback to Scylla API proxy
	priv.NotFound(apiProxy(c))

	return r
}

func promProxy(c agent.Config) http.Handler {
	addr := c.Scylla.PrometheusAddress
	if addr == "" {
		addr = c.Scylla.ListenAddress
	}
	return &httputil.ReverseProxy{
		Director: director(net.JoinHostPort(addr, c.Scylla.PrometheusPort)),
	}
}

func apiProxy(c agent.Config) http.HandlerFunc {
	h := &httputil.ReverseProxy{
		Director: director(net.JoinHostPort(c.Scylla.APIAddress, c.Scylla.APIPort)),
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
