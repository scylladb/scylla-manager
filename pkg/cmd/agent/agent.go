// Copyright (C) 2017 ScyllaDB

package main

import (
	"net/http"
	"runtime"
	"runtime/debug"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

func newAgentHandler(c agent.Config, rclone http.Handler) *chi.Mux {
	m := chi.NewMux()

	m.Get("/node_info", newNodeInfoHandler(c).getNodeInfo)
	m.Get("/mem_stats", memStats())
	m.Post("/terminate", selfSigterm())
	m.Post("/free_os_memory", func(writer http.ResponseWriter, request *http.Request) {
		debug.FreeOSMemory()
	})

	// Rclone server
	m.Mount("/rclone", http.StripPrefix("/agent/rclone", rclone))

	return m
}

func memStats() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := &runtime.MemStats{}
		runtime.ReadMemStats(ms)
		render.Status(r, http.StatusOK)
		render.Respond(w, r, &models.MemStats{
			Alloc:        int64(ms.Alloc),
			Frees:        int64(ms.Frees),
			HeapIdle:     int64(ms.HeapIdle),
			HeapInuse:    int64(ms.HeapInuse),
			HeapReleased: int64(ms.HeapReleased),
			HeapSys:      int64(ms.HeapSys),
			Mallocs:      int64(ms.Mallocs),
			OtherSys:     int64(ms.OtherSys),
			Sys:          int64(ms.Sys),
			TotalAlloc:   int64(ms.TotalAlloc),
		})
	}
}

func selfSigterm() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.Respond(w, r, errors.Wrap(err, "kill"))
		}
	}
}
