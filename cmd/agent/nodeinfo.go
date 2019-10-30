// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/http"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/scyllaclient"
)

func (mux *router) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	addr := net.JoinHostPort(mux.config.Scylla.APIAddress, mux.config.Scylla.APIPort)
	client := scyllaclient.NewConfigClient(addr)

	nodeInfo, err := client.NodeInfo(r.Context())
	if err != nil {
		render.Respond(w, r, errors.Wrap(err, "node info fetch"))
		return
	}

	render.Respond(w, r, nodeInfo)
}
