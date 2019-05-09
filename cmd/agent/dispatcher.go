// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

const defaultAPIPort = "10000"

type dispatcher struct {
	client *http.Client
}

func (d dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, port, _ := net.SplitHostPort(r.Host)
	if port == "" {
		port = defaultAPIPort
	}

	d.sendRequest(w, withHost(r, "0.0.0.0:"+port))
}

func (d dispatcher) sendRequest(w http.ResponseWriter, r *http.Request) {
	resp, err := d.client.Do(r)
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

// clone request creates a new client request from server request.
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
