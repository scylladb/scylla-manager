// Copyright (C) 2017 ScyllaDB

package main

import (
	"net/http"
	"runtime/debug"

	"github.com/go-chi/chi"
)

func newAgentHandler(config config, rclone http.Handler) *chi.Mux {
	m := chi.NewMux()

	m.Get("/node_info", newNodeInfoHandler(config).getNodeInfo)
	m.Post("/free_os_memory", func(writer http.ResponseWriter, request *http.Request) {
		debug.FreeOSMemory()
	})

	// Rclone server
	m.Mount("/rclone", http.StripPrefix("/agent/rclone", rclone))

	return m
}
