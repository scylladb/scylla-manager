// Copyright (C) 2017 ScyllaDB

package main

import (
	"net/http"
	"runtime/debug"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
	"github.com/scylladb/scylla-manager/v3/pkg/util/cpuset"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

func newAgentHandler(c agent.Config, rclone http.Handler, cloudMeta http.HandlerFunc, logger log.Logger) *chi.Mux {
	m := chi.NewMux()

	m.Get("/node_info", newNodeInfoHandler(c).getNodeInfo)
	m.Get("/pin_cpu", func(writer http.ResponseWriter, request *http.Request) {
		cpus, err := cpuset.SchedGetAffinity()
		if err != nil {
			render.Status(request, http.StatusInternalServerError)
			render.Respond(writer, request, err)
		}
		casted := make([]int64, len(cpus))
		for i := range cpus {
			casted[i] = int64(cpus[i])
		}
		render.Respond(writer, request, &models.Cpus{CPU: casted})
	})
	m.Post("/pin_cpu", func(writer http.ResponseWriter, request *http.Request) {
		if err := findAndPinCPUs(request.Context(), c, logger); err != nil {
			render.Status(request, http.StatusInternalServerError)
			render.Respond(writer, request, err)
		}
	})
	m.Delete("/pin_cpu", func(writer http.ResponseWriter, request *http.Request) {
		if err := unpinFromCPUs(); err != nil {
			render.Status(request, http.StatusInternalServerError)
			render.Respond(writer, request, err)
		}
	})
	m.Post("/terminate", selfSigterm())
	m.Post("/free_os_memory", func(_ http.ResponseWriter, _ *http.Request) {
		debug.FreeOSMemory()
	})

	m.Get("/cloud/metadata", cloudMeta)

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
