// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service/repair"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

type repairTestHelper struct {
	session gocqlx.Session
	hrt     *HackableRoundTripper
	client  *scyllaclient.Client
	service *repair.Service

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newRepairTestHelper(t *testing.T, session gocqlx.Session, config repair.Config) *repairTestHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.DebugLevel)

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	c := newTestClient(t, hrt, logger)
	s := newTestService(t, session, c, config, logger)

	return &repairTestHelper{
		session: session,
		hrt:     hrt,
		client:  c,
		service: s,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func newTestClient(t *testing.T, hrt *HackableRoundTripper, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	config.Transport = hrt

	c, err := scyllaclient.NewClient(config, logger.Named("scylla"))
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c repair.Config, logger log.Logger) *repair.Service {
	t.Helper()

	s, err := repair.NewService(
		session,
		c,
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func TestServiceGetTargetIntegration(t *testing.T) {
	// Clear keyspaces
	CreateManagedClusterSession(t)

	// Test names
	testNames := []string{
		"everything",
		"filter keyspaces",
		"filter dc",
		"continue",
		"filter tables",
		"fail_fast",
		"complex",
		"no enough replicas",
	}

	var (
		session = CreateSession(t)
		h       = newRepairTestHelper(t, session, repair.DefaultConfig())
		ctx     = context.Background()
	)

	for _, testName := range testNames {
		t.Run(testName, func(t *testing.T) {
			input := ReadInputFile(t)
			v, err := h.service.GetTarget(ctx, h.clusterID, input, false)
			if err != nil {
				t.Fatal(err)
			}

			SaveGoldenJSONFileIfNeeded(t, v)

			var golden repair.Target
			LoadGoldenJSONFile(t, &golden)

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	var (
		session = CreateSession(t)
		h       = newRepairTestHelper(t, session, repair.DefaultConfig())
		ctx     = context.Background()
	)

	target, err := h.service.GetTarget(ctx, h.clusterID, json.RawMessage("{}"), false)
	if err != nil {
		t.Fatal("GetTarget() failed", err)
	}
	target.Intensity = 50

	err = h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, target)
	if err != nil {
		t.Fatal("Repair() failed", err)
	}
}
