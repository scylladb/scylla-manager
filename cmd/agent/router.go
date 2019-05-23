// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/pkg/errors"
)

const host = "0.0.0.0"

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
	p := path.Clean(r.URL.Path) + "/"
	switch {
	case strings.HasPrefix(p, "/rclone/"):
		// Stripping prefix to use clean paths in rclone server.
		// Eg. "/rclone/operations/about" to "/operations/about".
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/rclone")
		mux.rclone.ServeHTTP(w, r)
	case strings.HasPrefix(p, "/metrics/"):
		mux.sendRequest(w, withHost(r, host+":"+mux.config.Scylla.PrometheusPort))
	default:
		mux.sendRequest(w, withHost(r, host+":"+mux.config.Scylla.APIPort))
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

func withHost(r *http.Request, host string) *http.Request {
	req := cloneRequest(r)
	req.Host = host
	req.URL.Host = host
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
