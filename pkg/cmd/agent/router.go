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
	"github.com/scylladb/scylla-manager/pkg/auth"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/httpexec"
	"github.com/scylladb/scylla-manager/pkg/util/httphandler"
	"github.com/scylladb/scylla-manager/pkg/util/httplog"
)

var unauthorizedErrorBody = json.RawMessage(`{"message":"unauthorized","code":401}`)

func newRouter(c config.AgentConfig, rclone http.Handler, logger log.Logger) http.Handler {
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
		auth.ValidateToken(c.AuthToken, time.Second, unauthorizedErrorBody),
	)
	// Agent specific endpoints
	priv.Mount("/agent", newAgentHandler(c, rclone))
	// Scylla prometheus proxy
	priv.Mount("/metrics", promProxy(c))
	// Bash as-a-service
	priv.Mount("/exec", httpexec.NewBashServer(logger.Named("exec")))
	// Fallback to Scylla API proxy
	priv.NotFound(apiProxy(c))

	return r
}

func promProxy(c config.AgentConfig) http.Handler {
	return &httputil.ReverseProxy{
		Director: director(net.JoinHostPort(c.Scylla.PrometheusAddress, c.Scylla.PrometheusPort)),
	}
}

func apiProxy(c config.AgentConfig) http.HandlerFunc {
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
