// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
)

type clusterFilter struct {
	svc ClusterService
}

func (h clusterFilter) clusterCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.svc == nil {
			next.ServeHTTP(w, r)
			return
		}

		clusterID := chi.URLParam(r, "cluster_id")
		if clusterID == "" {
			respondBadRequest(w, r, errors.New("missing cluster ID"))
			return
		}

		c, err := h.svc.GetCluster(r.Context(), clusterID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "load cluster %q", clusterID))
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxClusterID, c.ID)
		ctx = context.WithValue(ctx, ctxCluster, c)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type clusterHandler clusterFilter

func newClusterHandler(svc ClusterService) *chi.Mux {
	m := chi.NewMux()
	h := clusterHandler{
		svc: svc,
	}

	m.Route("/clusters", func(r chi.Router) {
		r.Get("/", h.listClusters)
		r.Post("/", h.createCluster)
	})
	m.Route("/cluster/{cluster_id}", func(r chi.Router) {
		r.Use(clusterFilter(h).clusterCtx)
		r.Get("/", h.loadCluster)
		r.Put("/", h.updateCluster)
		r.Delete("/", h.deleteCluster)
	})
	return m
}

func (h clusterHandler) listClusters(w http.ResponseWriter, r *http.Request) {
	ids, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "list clusters"))
		return
	}

	if len(ids) == 0 {
		render.Respond(w, r, []struct{}{})
		return
	}
	render.Respond(w, r, ids)
}

func (h clusterHandler) parseCluster(r *http.Request) (*cluster.Cluster, error) {
	var c cluster.Cluster
	if err := render.DecodeJSON(r.Body, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (h clusterHandler) createCluster(w http.ResponseWriter, r *http.Request) {
	newCluster, err := h.parseCluster(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.PutCluster(r.Context(), newCluster); err != nil {
		respondError(w, r, errors.Wrap(err, "create cluster"))
		return
	}

	location := r.URL.ResolveReference(&url.URL{
		Path: path.Join("cluster", newCluster.ID.String()),
	})
	w.Header().Set("Location", location.String())
	w.WriteHeader(http.StatusCreated)
}

func (h clusterHandler) loadCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)
	render.Respond(w, r, c)
}

func (h clusterHandler) updateCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	newCluster, err := h.parseCluster(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	newCluster.ID = c.ID

	if err := h.svc.PutCluster(r.Context(), newCluster); err != nil {
		respondError(w, r, errors.Wrapf(err, "update cluster %q", c.ID))
		return
	}
	render.Respond(w, r, newCluster)
}

func (h clusterHandler) deleteCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	var (
		deleteCQLCredentials bool
		deleteSSLUserCert    bool
		err                  error
	)

	if v := r.FormValue("cql_creds"); v != "" {
		deleteCQLCredentials, err = strconv.ParseBool(v)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}
	if v := r.FormValue("ssl_user_cert"); v != "" {
		deleteSSLUserCert, err = strconv.ParseBool(v)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}

	if !deleteCQLCredentials && !deleteSSLUserCert {
		if err := h.svc.DeleteCluster(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete cluster %q", c.ID))
			return
		}
	}
	if deleteCQLCredentials {
		if err := h.svc.DeleteCQLCredentials(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete CQL credentials for cluster %q", c.ID))
			return
		}
	}
	if deleteSSLUserCert {
		if err := h.svc.DeleteSSLUserCert(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete SSL user cert for cluster %q", c.ID))
			return
		}
	}
}
