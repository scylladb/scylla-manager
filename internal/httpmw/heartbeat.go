// Copyright (C) 2017 ScyllaDB

package httpmw

import "net/http"

// HeartbeatHandler responds with status 204.
func HeartbeatHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
}
