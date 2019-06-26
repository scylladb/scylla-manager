// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/pkg/errors"
)

type router struct {
	config config
	rclone http.Handler
	client *http.Client
}

func newRouter(config config, rclone http.Handler, client *http.Client) *router {
	return &router{
		config: config,
		rclone: rclone,
		client: client,
	}
}

func (mux *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := mux.config.Scylla
	p := path.Clean(r.URL.Path) + "/"
	switch {
	case strings.HasPrefix(p, "/rclone/"):
		// Stripping prefix to use clean paths in rclone server
		// eg. "/rclone/operations/about" to "/operations/about".
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/rclone")
		mux.rclone.ServeHTTP(w, r)
	case strings.HasPrefix(p, "/metrics/"):
		mux.sendRequest(w, withHostPort(r, s.PrometheusAddress, s.PrometheusPort))
	default:
		mux.sendRequest(w, withHostPort(r, s.APIAddress, s.APIPort))
	}
}

func (mux *router) sendRequest(w http.ResponseWriter, r *http.Request) {
	resp, err := mux.client.Do(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errors.Wrap(err, "proxy error").Error())) // nolint: errcheck
	}

	// Headers
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	// Body
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("copy error: %+v", err)
	}
}

func withHostPort(r *http.Request, host, port string) *http.Request {
	hp := net.JoinHostPort(host, port)
	req := cloneRequest(r)
	req.Host = hp
	req.URL.Host = hp
	req.URL.Scheme = "http"
	return req
}

// clone request creates a new client request from router request.
func cloneRequest(r *http.Request) *http.Request {
	// New copy basic fields, same that are set with http.NewRequest
	req := &http.Request{
		Method:     r.Method,
		URL:        new(url.URL),
		Proto:      r.Proto,
		ProtoMajor: r.ProtoMajor,
		ProtoMinor: r.ProtoMinor,
		Header:     make(http.Header),
		Body:       r.Body,
	}

	// Deep copy URL
	*req.URL = *r.URL

	// Deep copy headers
	copyHeader(req.Header, r.Header)

	return req
}

func copyHeader(dst, src http.Header) {
	for k, v := range src {
		vv := make([]string, len(v))
		copy(vv, v)
		dst[k] = vv
	}
}
