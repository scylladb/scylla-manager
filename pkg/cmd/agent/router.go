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

// scyllaAPIPathParam is a list of known scylla REST API endpoints used by SM
// which contain path params (captured in curly brackets, e.g., {param}) with
// unbounded cardinality (e.g., task ID, table name, etc.). This is the best
// effort list which needs to be updated manually when SM starts using new
// scylla endpoint matching the mentioned criteria.
var scyllaAPIPathParam = []string{
	"/column_family/autocompaction/{name}",
	"/column_family/metrics/total_disk_space_used/{name}",
	"/storage_service/describe_ring/{keyspace}",
	"/storage_service/keyspace_flush/{keyspace}",
	"/storage_service/repair_async/{keyspace}",
	"/storage_service/sstables/{keyspace}",
	"/storage_service/tokens/{endpoint}",
	"/storage_service/view_build_statuses/{keyspace}/{view}",
	"/task_manager/abort_task/{task_id}",
	"/task_manager/task_status/{task_id}",
	"/task_manager/wait_task/{task_id}",
}

func newRouter(c agent.Config, metrics AgentMetrics, rclone http.Handler, cloudMeta http.HandlerFunc, logger log.Logger) http.Handler {
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
	priv.Mount("/agent", newAgentHandler(c, rclone, cloudMeta, logger.Named("agent")))
	// Scylla prometheus proxy
	priv.Mount("/metrics", promProxy(c))
	// Register endpoints with path params separately, so that
	// router records the pattern used to match given request.
	// This is needed by metricsPath.
	api := apiProxy(c)
	for _, pattern := range scyllaAPIPathParam {
		priv.Handle(pattern, api)
	}
	// Fallback to Scylla API proxy
	priv.NotFound(api)

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
