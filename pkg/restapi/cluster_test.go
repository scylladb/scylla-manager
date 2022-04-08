// Copyright (C) 2017 ScyllaDB

//go:generate mockgen -destination mock_clusterservice_test.go -mock_names ClusterService=MockClusterService -package restapi github.com/scylladb/scylla-manager/pkg/restapi ClusterService

package restapi_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/restapi"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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
	m.EXPECT().PutCluster(gomock.Any(), NewClusterMatcher(&cluster.Cluster{ID: id})).Return(nil)

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

func TestClusterDeleteCQLCredentials(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteCQLCredentials(gomock.Any(), id).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), nil)
	r.URL.RawQuery = "cql_creds=1"
	r.ParseForm()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}
}

func TestClusterDeleteSSLUserCert(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteSSLUserCert(gomock.Any(), id).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), nil)
	r.URL.RawQuery = "ssl_user_cert=1"
	r.ParseForm()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}
}

// ClusterMatcher gomock.Matcher interface implementation for cluster.Cluster.
type ClusterMatcher struct {
	expected *cluster.Cluster
}

// NewClusterMatcher returns gomock.Matcher for clusters. It compares only ID field.
func NewClusterMatcher(expected *cluster.Cluster) *ClusterMatcher {
	return &ClusterMatcher{
		expected: expected,
	}
}

// Matches returns whether v is a match.
func (m ClusterMatcher) Matches(v interface{}) bool {
	c, ok := v.(*cluster.Cluster)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected.ID, c.ID, testutils.UUIDComparer())
}

func (m ClusterMatcher) String() string {
	return fmt.Sprintf("is equal to cluster with ID: %s", m.expected.ID.String())
}
