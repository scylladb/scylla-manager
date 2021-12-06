// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

type nodeInfoHandler struct {
	config config.AgentConfig
}

func newNodeInfoHandler(c config.AgentConfig) *nodeInfoHandler {
	return &nodeInfoHandler{config: c}
}

func (h *nodeInfoHandler) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	configClient := scyllaclient.NewConfigClient(h.APIAddr())

	nodeInfo, err := configClient.NodeInfo(ctx)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.Respond(w, r, errors.Wrap(err, "node info fetch"))
		return
	}

	if err := h.sysInfo(nodeInfo); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.Respond(w, r, errors.Wrap(err, "sys info"))
		return
	}

	if err := h.versionInfo(ctx, nodeInfo); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.Respond(w, r, errors.Wrap(err, "version info"))
		return
	}

	render.Respond(w, r, nodeInfo)
}

func (h *nodeInfoHandler) versionInfo(ctx context.Context, info *scyllaclient.NodeInfo) error {
	info.AgentVersion = pkg.Version()

	scyllaVersion, err := h.getScyllaVersion(ctx)
	if err != nil {
		return err
	}
	info.ScyllaVersion = scyllaVersion

	return nil
}

func (h *nodeInfoHandler) getScyllaVersion(ctx context.Context) (string, error) {
	u := url.URL{
		Host:   h.APIAddr(),
		Scheme: "http",
		Path:   "/storage_service/scylla_release_version",
	}

	c := http.DefaultClient
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// Scylla API returns quoted version string.
	return strconv.Unquote(string(buf))
}

func (h *nodeInfoHandler) APIAddr() string {
	return net.JoinHostPort(h.config.Scylla.APIAddress, h.config.Scylla.APIPort)
}
