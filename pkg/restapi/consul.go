// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
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
		r.Get("/service/scylla", h.getCatalogNodes)
	})
	m.Route("/health", func(r chi.Router) {
		r.Get("/service/scylla", h.getHealthNodes)
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
		respondError(w, r, errors.Wrap(err, "list clusters"))
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
	ID              string
	Node            string
	Address         string
	Datacenter      string
	TaggedAddresses map[string]string
	NodeMeta        map[string]string
	ServiceKind     string
	ServiceID       string
	ServiceName     string
	ServiceTags     []string
	ServiceAddress  string
	ServiceWeights  struct {
		Passing int
		Warning int
	}
	ServiceMeta              map[string]string
	ServicePort              int
	ServiceEnableTagOverride bool
	ServiceProxyDestination  string
	ServiceProxy             map[string]interface{}
	ServiceConnect           map[string]interface{}
	CreateIndex              uint64
	ModifyIndex              uint64
}

// https://www.consul.io/api-docs/health#sample-response-2
type consulHealthNode struct {
	ID              string
	Node            string
	Address         string
	Datacenter      string
	TaggedAddresses map[string]string
	Meta            map[string]string
	CreateIndex     uint64
	ModifyIndex     uint64
}

// https://www.consul.io/api-docs/health#sample-response-2
type consulService struct {
	ID              string
	Service         string
	Tags            []string
	Address         string
	TaggedAddresses map[string]string
	Meta            map[string]string
	Port            int
	Weights         map[string]int
	Namespace       string
}

// https://www.consul.io/api-docs/health#sample-response-2
type consulHealthServiceItem struct {
	Node    consulHealthNode
	Service consulService
	Checks  []interface{}
}

func (h *consulHandler) getCatalogNodes(w http.ResponseWriter, r *http.Request) {
	clusters, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "list clusters"))
		return
	}
	var result []consulNode
	for _, c := range clusters {
		nodes, err := h.svc.ListNodes(r.Context(), c.ID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "list nodes for cluster %q", c.ID))
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
					"shard_num":    strconv.Itoa(int(n.ShardNum)),
					"dc":           n.Datacenter,
					"cluster_name": c.String(),
				},
			}
			result = append(result, cn)
		}
	}
	h.index++
	w.Header().Add("X-Consul-Index", strconv.Itoa(h.index))

	render.Respond(w, r, result)
}

func (h *consulHandler) getHealthNodes(w http.ResponseWriter, r *http.Request) {
	clusters, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "list clusters"))
		return
	}
	var result []consulHealthServiceItem
	for _, c := range clusters {
		nodes, err := h.svc.ListNodes(r.Context(), c.ID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "list nodes for cluster %q", c.ID))
			return
		}
		for _, n := range nodes {
			cn := consulHealthServiceItem{
				Node: consulHealthNode{
					Node:       n.Address,
					Address:    n.Address,
					Datacenter: n.Datacenter,
					Meta: map[string]string{
						"shard_num":    strconv.Itoa(int(n.ShardNum)),
						"dc":           n.Datacenter,
						"cluster_name": c.String(),
					},
				},
				Service: consulService{
					Address: n.Address,
					ID:      "scylla",
					Service: "scylla",
					Port:    9180,
					Tags:    []string{c.String()},
					Meta: map[string]string{
						"dc":           n.Datacenter,
						"cluster_name": c.String(),
					},
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
