// Copyright (C) 2017 ScyllaDB

package main

import (
	"net/http"
	"runtime/debug"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
)

func newAgentHandler(config config, rclone http.Handler) *chi.Mux {
	m := chi.NewMux()

	m.Get("/node_info", newNodeInfoHandler(config).getNodeInfo)
	m.Post("/terminate", selfSigterm())
	m.Post("/free_os_memory", func(writer http.ResponseWriter, request *http.Request) {
		debug.FreeOSMemory()
	})

	// Rclone server
	m.Mount("/rclone", http.StripPrefix("/agent/rclone", rclone))

	return m
}

func selfSigterm() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.Respond(w, r, errors.Wrap(err, "kill"))
		}
	}
}
