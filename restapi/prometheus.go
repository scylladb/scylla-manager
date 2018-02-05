// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// prometheusMiddleware exposes promhttp.Handler as chi middleware. This is
// implemented this way to avoid interference with other middleware.
func prometheusMiddleware(endpoint string) func(http.Handler) http.Handler {
	f := func(next http.Handler) http.Handler {
		h := promhttp.Handler()

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.EqualFold(r.URL.Path, endpoint) {
				h.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	return f
}
