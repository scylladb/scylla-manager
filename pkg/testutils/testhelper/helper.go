// Copyright (C) 2023 ScyllaDB

package testhelper

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// CommonTestHelper common tester object for backups and repairs.
type CommonTestHelper struct {
	Logger  log.Logger
	Session gocqlx.Session
	Hrt     *testutils.HackableRoundTripper
	Client  *scyllaclient.Client

	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	T         *testing.T
}

// GetHostsFromDC returns list of hosts on the scylla cluster on the given DC.
func (h *CommonTestHelper) GetHostsFromDC(dcName string) []string {
	info, err := h.Client.Datacenters(context.Background())
	if err != nil {
		h.T.Fatal(err)
	}
	return info[dcName]
}

// GetAllHosts returns list of hosts on the scylla cluster across all DC available.
func (h *CommonTestHelper) GetAllHosts() []string {
	info, err := h.Client.Datacenters(context.Background())
	if err != nil {
		h.T.Fatal(err)
	}
	var out []string
	for _, dcList := range info {
		out = append(out, dcList...)
	}
	return out
}
