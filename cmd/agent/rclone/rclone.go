// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"net/http"

	"github.com/go-chi/chi"
)

// NewHandler returns rclone http.Handler.
func NewHandler() http.Handler {
	mux := chi.NewRouter()
	return mux
}
