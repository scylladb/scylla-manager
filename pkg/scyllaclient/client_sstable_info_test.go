// Copyright (C) 2026 ScyllaDB

package scyllaclient_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestClientSSTableInfo(t *testing.T) {
	t.Parallel()

	var gotKeyspace, gotTable string
	m := func(req *http.Request) string {
		if req.URL.Path != "/storage_service/sstable_info" {
			return ""
		}
		gotKeyspace = req.URL.Query().Get("keyspace")
		gotTable = req.URL.Query().Get("cf")
		return "testdata/scylla_api/storage_service_sstable_info.json"
	}
	client, closeServer := scyllaclienttest.NewFakeScyllaServerMatching(t, m)
	defer closeServer()

	out, err := client.SSTableInfo(context.Background(), scyllaclienttest.TestHost, "ks", "tb")
	if err != nil {
		t.Fatal(err)
	}
	if gotKeyspace != "ks" || gotTable != "tb" {
		t.Fatalf("query: keyspace=%q cf=%q", gotKeyspace, gotTable)
	}
	if len(out) != 1 || len(out[0].SSTables) != 2 {
		t.Fatalf("unexpected response: %+v", out)
	}
	groups := out[0].SSTables[0].ExtendedProperties
	if len(groups) != 2 || groups[1].Group != "encryption_info" {
		t.Fatalf("unexpected extended properties: %+v", groups)
	}
}
