// Copyright (C) 2017 ScyllaDB

package restapi

import "net/http"

// Heartbeat responds with status 204.
func Heartbeat() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
}
