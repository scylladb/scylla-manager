// Copyright (C) 2017 ScyllaDB

//go:generate mockgen -destination mock_clusterservice_test.go -mock_names ClusterService=MockClusterService -package restapi github.com/scylladb/mermaid/restapi ClusterService

package restapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/service/cluster"
	"github.com/scylladb/mermaid/uuid"
)

func TestClusterList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expected := []*cluster.Cluster{{ID: uuid.MustRandom(), Name: "name"}}

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().ListClusters(gomock.Any(), &cluster.Filter{}).Return(expected, nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assertJsonBody(t, w, expected)
}

func TestClusterCreateGeneratesIDWhenNotProvided(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().PutCluster(gomock.Any(), &cluster.Cluster{Name: "name"}).Do(func(_ interface{}, e *cluster.Cluster) {
		e.ID = id
	}).Return(nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", jsonBody(t, &cluster.Cluster{Name: "name"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}

	if !strings.Contains(w.Header().Get("Location"), id.String()) {
		t.Fatal(w.Header())
	}
}

func TestClusterCreateWithProvidedID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().PutCluster(gomock.Any(), mermaidtest.NewClusterMatcher(&cluster.Cluster{ID: id})).Return(nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", jsonBody(t, &cluster.Cluster{ID: id}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}

	if !strings.Contains(w.Header().Get("Location"), id.String()) {
		t.Fatal(w.Header())
	}
}
