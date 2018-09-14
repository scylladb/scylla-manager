// Copyright (C) 2017 ScyllaDB

//go:generate mockgen -destination mock_clusterservice_test.go -mock_names ClusterService=MockClusterService -package restapi github.com/scylladb/mermaid/restapi ClusterService

package restapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/uuid"
)

func jsonBody(t testing.TB, v interface{}) *bytes.Reader {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(b)
	}
	return bytes.NewReader(b)
}

func assertJsonBody(t testing.TB, w *httptest.ResponseRecorder, expected interface{}) {
	b, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	actual := strings.TrimSpace(w.Body.String())

	if diff := cmp.Diff(actual, string(b)); diff != "" {
		t.Fatal(diff)
	}
}

func TestClusterList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expected := []*cluster.Cluster{{ID: uuid.MustRandom(), Name: "name"}}

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().ListClusters(gomock.Any(), &cluster.Filter{}).Return(expected, nil)

	h := restapi.New(&restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assertJsonBody(t, w, expected)
}

func TestClusterCreate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().PutCluster(gomock.Any(), &cluster.Cluster{Name: "name"}).Do(func(_ interface{}, e *cluster.Cluster) {
		e.ID = id
	}).Return(nil)

	h := restapi.New(&restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", jsonBody(t, &cluster.Cluster{Name: "name"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if !strings.Contains(w.Header().Get("Location"), id.String()) {
		t.Fatal(w.Header())
	}
}
