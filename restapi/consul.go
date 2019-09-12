// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/service/cluster"
	"github.com/scylladb/mermaid/uuid"
)

func newConsulHandler(cs ClusterService) *chi.Mux {
	m := chi.NewMux()
	h := &consulHandler{
		svc: cs,
	}

	// Consul plugin for Prometheus uses three endpoints:
	// - /v1/agent/self for getting datacenter name of the agent itself.
	// - /v1/catalog/services for listing all available services.
	// - /v1/catalog/service/scylla to list all available nodes.
	// We are providing that interface here to allow Prometheus to discover
	// clusters.
	m.Get("/agent/self", h.getAgent)
	m.Route("/catalog", func(r chi.Router) {
		r.Get("/services", h.listServices)
		r.Get("/service/scylla", h.getNodes)
	})

	return m
}

type consulHandler struct {
	svc   ClusterService
	index int
}

func (h *consulHandler) listServices(w http.ResponseWriter, r *http.Request) {
	clusters, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "failed to list clusters"))
		return
	}
	tags := make([]string, 0, len(clusters))
	for _, c := range clusters {
		tags = append(tags, c.String())
	}
	render.Respond(w, r, map[string][]string{
		"scylla": tags,
	})
}

// Constructed from:
// https://www.consul.io/api/catalog.html#sample-response-3
type consulNode struct {
	ID              uuid.UUID
	Node            string
	Address         string
	Datacenter      string
	TaggedAddresses struct {
		Lan string `json:"lan"`
		Wan string `json:"wan"`
	}
	NodeMeta       map[string]interface{}
	ServiceKind    string
	ServiceID      string
	ServiceName    string
	ServiceTags    []string
	ServiceAddress string
	ServiceWeights struct {
		Passing int
		Warning int
	}
	ServiceMeta              map[string]string
	ServicePort              int
	ServiceEnableTagOverride bool
	ServiceProxyDestination  string
	ServiceProxy             map[string]interface{}
	ServiceConnect           map[string]interface{}
	CreateIndex              int
	ModifyIndex              int
}

func (h *consulHandler) getNodes(w http.ResponseWriter, r *http.Request) {
	clusters, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "failed to list clusters"))
		return
	}
	result := []consulNode{}
	for _, c := range clusters {
		nodes, err := h.svc.ListNodes(r.Context(), c.ID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "failed to list nodes for cluster %q", c.ID))
			return
		}
		for _, n := range nodes {
			cn := consulNode{
				Datacenter:     n.Datacenter,
				Node:           n.Address,
				Address:        n.Address,
				ServiceAddress: n.Address,
				ServiceID:      "scylla",
				ServiceName:    "scylla",
				ServicePort:    9180,
				ServiceTags:    []string{c.String()},
				ServiceMeta: map[string]string{
					"shard_num": strconv.Itoa(int(n.ShardNum)),
					"dc":        n.Datacenter,
				},
			}
			result = append(result, cn)
		}
	}
	h.index++
	w.Header().Add("X-Consul-Index", strconv.Itoa(h.index))

	render.Respond(w, r, result)
}

func (h *consulHandler) getAgent(w http.ResponseWriter, r *http.Request) {
	render.Respond(w, r, map[string]interface{}{
		"Config": map[string]interface{}{"Datacenter": "scylla-manager"},
	})
}
