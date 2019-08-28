// Copyright (C) 2017 ScyllaDB

package pprof

import (
	"net/http"
	"net/http/pprof"
)

// Handler returns http.Handler serving pprof endpoints.
func Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/debug/pprof/", pprof.Index)
	m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	m.HandleFunc("/debug/pprof/profile", pprof.Profile)
	m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	m.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return m
}
