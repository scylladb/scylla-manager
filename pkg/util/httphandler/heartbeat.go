// Copyright (C) 2017 ScyllaDB

package httphandler

import "net/http"

// Heartbeat responds with status 204.
func Heartbeat() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
}
