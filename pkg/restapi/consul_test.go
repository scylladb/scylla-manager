// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/service/cluster"
	"github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func TestConsulAPI(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clusters := []*cluster.Cluster{
		{
			ID:   uuid.MustRandom(),
			Name: "cluster1",
		},
	}

	t.Run("list services", func(t *testing.T) {
		m := NewMockClusterService(ctrl)
		m.EXPECT().ListClusters(gomock.Any(), &cluster.Filter{}).Return(clusters, nil)

		h := NewPrometheus(m, &MetricsWatcher{})
		r := httptest.NewRequest(http.MethodGet, "/v1/catalog/services", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatal(w.Code)
		}

		expected := map[string][]string{
			"scylla": []string{"cluster1"},
		}
		got := map[string][]string{}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err.Error())
		}

		if diff := cmp.Diff(expected, got); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("list service nodes", func(t *testing.T) {
		nodes := []cluster.Node{
			{
				"dc1",
				"127.0.0.1",
				2,
			},
			{
				"dc2",
				"127.0.0.1",
				3,
			},
		}
		m := NewMockClusterService(ctrl)
		m.EXPECT().ListClusters(gomock.Any(), &cluster.Filter{}).Return(clusters, nil)
		m.EXPECT().ListNodes(gomock.Any(), clusters[0].ID).Return(nodes, nil)
		h := NewPrometheus(m, &MetricsWatcher{})

		r := httptest.NewRequest(http.MethodGet, "/v1/catalog/service/scylla", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatal(w.Code)
		}
		expected := []consulNode{
			{
				Datacenter:     "dc1",
				Node:           "127.0.0.1",
				Address:        "127.0.0.1",
				ServiceAddress: "127.0.0.1",
				ServiceID:      "scylla",
				ServiceName:    "scylla",
				ServicePort:    9180,
				ServiceTags:    []string{"cluster1"},
				ServiceMeta: map[string]string{
					"shard_num":    "2",
					"dc":           "dc1",
					"cluster_name": "cluster1",
				},
			},
			{
				Datacenter:     "dc2",
				Node:           "127.0.0.1",
				Address:        "127.0.0.1",
				ServiceAddress: "127.0.0.1",
				ServiceID:      "scylla",
				ServiceName:    "scylla",
				ServicePort:    9180,
				ServiceTags:    []string{"cluster1"},
				ServiceMeta: map[string]string{
					"shard_num":    "3",
					"dc":           "dc2",
					"cluster_name": "cluster1",
				},
			},
		}
		got := []consulNode{}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err.Error())
		}
		if diff := cmp.Diff(got, expected, testutils.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
		if h, ok := w.HeaderMap["X-Consul-Index"]; !ok && h[0] != "1" {
			t.Fatalf("Wrong X-Consul-Index header %+v", w.HeaderMap)
		}
	})
}
