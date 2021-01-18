// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func givenCluster() *cluster.Cluster {
	return &cluster.Cluster{
		ID:   uuid.NewTime(),
		Name: "test-cluster",
	}
}

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
