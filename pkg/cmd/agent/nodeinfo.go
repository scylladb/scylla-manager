// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"sync"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"golang.org/x/sys/unix"
)

type nodeInfoHandler struct {
	config config

	cachedScyllaVersion string
	mu                  sync.Mutex
}

func newNodeInfoHandler(config config) *nodeInfoHandler {
	return &nodeInfoHandler{config: config}
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

func (h *nodeInfoHandler) sysInfo(info *scyllaclient.NodeInfo) error {
	si := unix.Sysinfo_t{}
	if err := unix.Sysinfo(&si); err != nil {
		return err
	}

	info.MemoryTotal = int64(si.Totalram)
	info.CPUCount = int64(runtime.NumCPU())
	info.Uptime = si.Uptime

	return nil
}

func (h *nodeInfoHandler) getScyllaVersion(ctx context.Context) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cachedScyllaVersion != "" {
		return h.cachedScyllaVersion, nil
	}

	u := url.URL{
		Host:   h.APIAddr(),
		Scheme: "http",
		Path:   "/storage_service/scylla_release_version",
	}

	c := http.DefaultClient
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// Scylla API returns quoted version string.
	scyllaVersion, err := strconv.Unquote(string(buf))
	if err != nil {
		return "", err
	}

	h.cachedScyllaVersion = scyllaVersion
	return h.cachedScyllaVersion, nil
}

func (h *nodeInfoHandler) APIAddr() string {
	return net.JoinHostPort(h.config.Scylla.APIAddress, h.config.Scylla.APIPort)
}
