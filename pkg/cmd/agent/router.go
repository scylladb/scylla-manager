// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/auth"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
	"github.com/scylladb/scylla-manager/v3/pkg/restapi"
)

var unauthorizedErrorBody = json.RawMessage(`{"message":"unauthorized","code":401}`)

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
	// Fallback to Scylla API proxy
	priv.NotFound(apiProxy(c, logger))

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

func apiProxy(c agent.Config, logger log.Logger) http.HandlerFunc {
	h := &httputil.ReverseProxy{
		Director: wrappedDirectors(
			director(net.JoinHostPort(c.Scylla.APIAddress, c.Scylla.APIPort)),
			objectStorageEndpointDirector(c, logger),
		),
	}
	return h.ServeHTTP
}

func wrappedDirectors(directors ...func(r *http.Request)) func(r *http.Request) {
	return func(r *http.Request) {
		for _, d := range directors {
			d(r)
		}
	}
}

func director(addr string) func(r *http.Request) {
	return func(r *http.Request) {
		r.Host = addr
		r.URL.Host = addr
		r.URL.Scheme = "http"
	}
}

const (
	endpointQueryKey = "endpoint"

	s3Provider    = "s3"
	gcsProvider   = "gcs"
	azureProvider = "azure"

	defaultGCSHost       = "storage.googleapis.com"
	defaultAzureEndpoint = "blob.core.windows.net"
)

// When working with Rclone, SM specifies just the provider name,
// and Rclone (with agent config) resolves it internally to the correct endpoint.
// This made it so user didn't need to specify the exact endpoint when running SM backup/restore tasks.
//
// When working with Scylla, SM needs to specify resolved host name on its own.
// This should be the same name as specified in 'object_storage.yaml'
// (See https://github.com/scylladb/scylladb/blob/92db2eca0b8ab0a4fa2571666a7fe2d2b07c697b/docs/dev/object_storage.md?plain=1#L29-L39).
//
// In order to maximize compatibility and UX, we still want it to be possible
// to specify just the provider name when running backup/restore.
// In such case, SM sends provider name as the "endpoint" query param,
// which is resolved by agent to proper host name when forwarding request to Scylla.
// Different "endpoint" query params are not resolved.
//
// Note that resolving "endpoint" query param in the proxy is just for the UX,
// so it might not work correctly in all the cases.
// In order to ensure correctness, "endpoint" should be specified directly by SM user
// so that no resolving is needed.
//
// ML_TODO: this is tmp solution (#4211) - verify that it's still needed before merging to master.
func objectStorageEndpointDirector(cfg agent.Config, logger log.Logger) func(r *http.Request) {
	regex := regexp.MustCompile(`^/storage_service/(backup|restore)$`)
	resolver := endpointResolver(cfg)

	return func(r *http.Request) {
		// Check for object storage endpoints
		if !regex.MatchString(r.URL.Path) {
			return
		}

		// Ensure the existence of "endpoint" query param
		q := r.URL.Query()
		if !q.Has(endpointQueryKey) {
			logger.Error(r.Context(), "Expected endpoint query param, but didn't receive it",
				"query", r.URL.RawQuery)
			return
		}

		// Resolve provider to the proper endpoint
		provider := q.Get(endpointQueryKey)
		resolvedEndpoint, err := resolver(provider)
		if err != nil {
			logger.Error(r.Context(), "Failed to resolve provider to endpoint", "provider", provider, "err", err)
			return
		}

		resolvedHost, err := endpointToHostName(resolvedEndpoint)
		if err != nil {
			logger.Error(r.Context(), "Failed to convert endpoint to host name",
				"endpoint", resolvedEndpoint,
				"err", err)
			return
		}

		q.Del(endpointQueryKey)
		q.Add(endpointQueryKey, resolvedHost)
		r.URL.RawQuery = q.Encode()
	}
}

func endpointResolver(cfg agent.Config) func(provider string) (string, error) {
	s3Resolver := s3EndpointResolver()

	return func(provider string) (string, error) {
		var resolvedEndpoint string
		switch provider {
		case s3Provider:
			resolvedEndpoint = cfg.S3.Endpoint
			if resolvedEndpoint == "" {
				var err error
				resolvedEndpoint, err = s3Resolver(cfg.S3.Region)
				if err != nil {
					return "", err
				}
			}
		case gcsProvider:
			// It's not possible to specify non-default GCS endpoint
			// with Rclone config (at least with Rclone v1.54).
			resolvedEndpoint = defaultGCSHost
		case azureProvider:
			// For Azure, account is a part of the resolved host
			if cfg.Azure.Account == "" {
				return "", errors.New("account is not set in Azure config")
			}
			endpoint := cfg.Azure.Endpoint
			if endpoint == "" {
				endpoint = defaultAzureEndpoint
			}
			resolvedEndpoint = cfg.Azure.Account + "." + endpoint
		default:
			// Endpoint has already been resolved on SM side
			resolvedEndpoint = provider
		}
		return resolvedEndpoint, nil
	}
}

func s3EndpointResolver() func(region string) (string, error) {
	var (
		// No need to resolve endpoint multiple times
		resolvedEndpoint string
		mu               sync.Mutex
	)

	return func(region string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if resolvedEndpoint != "" {
			return resolvedEndpoint, nil
		}

		resolver := endpoints.DefaultResolver()
		re, err := resolver.EndpointFor(s3Provider, region)
		if err != nil {
			return "", errors.Wrap(err, "resolve S3 endpoint for region "+region)
		}

		resolvedEndpoint = re.URL
		return re.URL, nil
	}
}

func endpointToHostName(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Wrap(err, "parse endpoint "+endpoint)
	}
	return u.Hostname(), nil
}
