// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/ssh"
	"github.com/scylladb/mermaid/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return nil, errors.New("not implemented")
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopment().Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	putRun := func(t *testing.T, r *repair.Run) {
		stmt, names := schema.RepairRun.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *repair.Run) {
		p := repair.RunProgress{
			ClusterID: r.ClusterID,
			UnitID:    r.UnitID,
			RunID:     r.ID,
			Host:      "172.16.1.3",
			Shard:     0,
		}
		stmt, names := schema.RepairRunProgress.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(&p).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("get global merged unit config", func(t *testing.T) {
		t.Parallel()

		c, err := s.GetMergedUnitConfig(ctx, &repair.Unit{
			ID:        uuid.MustRandom(),
			ClusterID: uuid.MustRandom(),
			Keyspace:  "keyspace",
		})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(&c.LegacyConfig, validConfig()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing config", func(t *testing.T) {
		t.Parallel()

		c, err := s.GetConfig(ctx, repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		})
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("put nil config", func(t *testing.T) {
		t.Parallel()

		if err := s.PutConfig(ctx, repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put invalid config", func(t *testing.T) {
		t.Parallel()

		invalid := -1
		c := validConfig()
		c.RetryLimit = &invalid

		if err := s.PutConfig(ctx, repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}, c); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("delete missing config", func(t *testing.T) {
		t.Parallel()

		if err := s.DeleteConfig(ctx, repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("put and get config", func(t *testing.T) {
		t.Parallel()

		src := repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}

		c0 := validConfig()
		c0.RetryLimit = nil
		c0.RetryBackoffSeconds = nil

		if err := s.PutConfig(ctx, src, c0); err != nil {
			t.Fatal(err)
		}
		c1, err := s.GetConfig(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c1); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("put and delete config", func(t *testing.T) {
		t.Parallel()

		src := repair.LegacyConfigSource{
			ClusterID:  uuid.MustRandom(),
			Type:       repair.UnitConfig,
			ExternalID: "id",
		}

		c := validConfig()

		if err := s.PutConfig(ctx, src, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteConfig(ctx, src); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetConfig(ctx, src)
		if err != mermaid.ErrNotFound {
			t.Fatal("expected nil")
		}
	})

	t.Run("list empty units", func(t *testing.T) {
		t.Parallel()

		units, err := s.ListUnits(ctx, uuid.MustRandom(), &repair.UnitFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(units) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list units", func(t *testing.T) {
		t.Parallel()

		id := uuid.MustRandom()

		expected := make([]*repair.Unit, 3)
		for i := range expected {
			u := &repair.Unit{
				ClusterID: id,
				ID:        uuid.NewTime(),
				Keyspace:  "keyspace" + strconv.Itoa(i),
				Tables: []string{
					fmt.Sprintf("table%d", 2*i),
					fmt.Sprintf("table%d", 2*i+1),
				},
			}
			if err := s.PutUnit(ctx, u); err != nil {
				t.Fatal(err)
			}
			expected[i] = u
		}

		units, err := s.ListUnits(ctx, id, &repair.UnitFilter{})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(units, expected, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing unit", func(t *testing.T) {
		t.Parallel()

		u, err := s.GetUnitByID(ctx, uuid.MustRandom(), uuid.MustRandom())
		if err != mermaid.ErrNotFound {
			t.Fatal("expected not found")
		}
		if u != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("get unit", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()
		u0.ID = uuid.Nil

		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}
		if u0.ID == uuid.Nil {
			t.Fatal("ID not updated")
		}
		u1, err := s.GetUnitByID(ctx, u0.ClusterID, u0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(u0, u1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}

		u2, err := s.GetUnitByName(ctx, u0.ClusterID, u0.Name)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(u0, u2, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("put nil unit", func(t *testing.T) {
		t.Parallel()

		if err := s.PutUnit(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put invalid unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		u.ClusterID = uuid.Nil

		if err := s.PutUnit(ctx, u); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put conflicting unit", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()

		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}

		u1 := u0
		u1.ID = uuid.Nil

		if err := s.PutUnit(ctx, u0); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("put new unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()
		u.ID = uuid.Nil

		if err := s.PutUnit(ctx, u); err != nil {
			t.Fatal(err)
		}
		if u.ID == uuid.Nil {
			t.Fatal("id not set")
		}
	})

	t.Run("delete missing unit", func(t *testing.T) {
		t.Parallel()

		err := s.DeleteUnit(ctx, uuid.MustRandom(), uuid.MustRandom())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete unit", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		if err := s.PutUnit(ctx, u); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteUnit(ctx, u.ClusterID, u.ID); err != nil {
			t.Fatal(err)
		}
		_, err := s.GetUnitByID(ctx, u.ClusterID, u.ID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("get last run", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		r, err := s.GetLastRun(ctx, u)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get last started run nothing to return", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r1)

		_, err := s.GetLastStartedRun(ctx, u)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("get last started run return first", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, u)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get last started run return first with error", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r0 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r1)
		putRunProgress(t, r1)

		r2 := &repair.Run{
			ClusterID: u.ClusterID,
			UnitID:    u.ID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, u)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("stop run", func(t *testing.T) {
		t.Parallel()

		u := validUnit()

		r := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u.ID,
			ClusterID: u.ClusterID,
			Status:    repair.StatusRunning,
		}

		putRun(t, &r)

		if err := s.StopRun(ctx, u, r.ID); err != nil {
			t.Fatal(err)
		}

		if run, err := s.GetRun(ctx, u, r.ID); err != nil {
			t.Fatal(err)
		} else if run.Status != repair.StatusStopping {
			t.Fatal(run.Status)
		}
	})

	t.Run("fix run status", func(t *testing.T) {
		t.Parallel()

		u0 := validUnit()
		if err := s.PutUnit(ctx, u0); err != nil {
			t.Fatal(err)
		}

		u1 := validUnit()
		if err := s.PutUnit(ctx, u1); err != nil {
			t.Fatal(err)
		}

		r0 := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u0.ID,
			ClusterID: u0.ClusterID,
			Status:    repair.StatusRunning,
		}
		putRun(t, &r0)

		r1 := repair.Run{
			ID:        uuid.NewTime(),
			UnitID:    u1.ID,
			ClusterID: u1.ClusterID,
			Status:    repair.StatusStopping,
		}
		putRun(t, &r1)

		if err := s.FixRunStatus(ctx); err != nil {
			t.Fatal(err)
		}

		if r, err := s.GetRun(ctx, u0, r0.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}

		if r, err := s.GetRun(ctx, u1, r1.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}
	})
}

func validConfig() *repair.LegacyConfig {
	enabled := true
	segmentSizeLimit := int64(-1)
	retryLimit := 3
	retryBackoffSeconds := 60
	parallelShardPercent := float32(1)

	return &repair.LegacyConfig{
		Enabled:              &enabled,
		SegmentSizeLimit:     &segmentSizeLimit,
		RetryLimit:           &retryLimit,
		RetryBackoffSeconds:  &retryBackoffSeconds,
		ParallelShardPercent: &parallelShardPercent,
	}
}

func validUnit() *repair.Unit {
	return &repair.Unit{
		ClusterID: uuid.MustRandom(),
		ID:        uuid.MustRandom(),
		Name:      "name",
		Keyspace:  "keyspace",
	}
}

func TestServiceSyncUnitsIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_0")
	createKeyspace(t, clusterSession, "test_1")
	createKeyspace(t, clusterSession, "test_2")

	s, _ := newTestService(t, session, repair.DefaultConfig())
	clusterID := uuid.MustRandom()
	ctx := context.Background()

	if err := s.PutUnit(ctx, &repair.Unit{ClusterID: clusterID, Keyspace: "test_2", Tables: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutUnit(ctx, &repair.Unit{ClusterID: clusterID, Keyspace: "test_2", Tables: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutUnit(ctx, &repair.Unit{ClusterID: clusterID, Keyspace: "test_missing"}); err != nil {
		t.Fatal(err)
	}

	if err := s.SyncUnits(ctx, clusterID); err != nil {
		t.Fatal(err)
	}

	units, err := s.ListUnits(ctx, clusterID, &repair.UnitFilter{})
	if err != nil {
		t.Fatal(err)
	}

	var actual []string
	for _, u := range units {
		actual = append(actual, u.Keyspace)
	}
	sort.Strings(actual)

	expected := []string{"test_0", "test_1", "test_2", "test_2"}

	if diff := cmp.Diff(actual, expected); diff != "" {
		t.Fatal(diff)
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	// fix values for testing...
	config := repair.DefaultConfig()
	config.SegmentsPerRepair = 5
	config.PollInterval = 100 * time.Millisecond
	config.ErrorBackoff = 1 * time.Second

	session := mermaidtest.CreateSession(t)
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	const (
		node0 = "172.16.1.3"
		node1 = "172.16.1.10"
	)

	var (
		s, hrt    = newTestService(t, session, config)
		clusterID = uuid.MustRandom()
		runID     = uuid.NewTime()
		unit      = repair.Unit{ClusterID: clusterID, Keyspace: "test_repair"}
		ctx       = context.Background()
	)

	assertStatus := func(expected repair.Status) {
		if r, err := s.GetRun(ctx, &unit, runID); err != nil {
			t.Fatal(err)
		} else if r.Status != expected {
			t.Fatal("wrong status", r, "expected", expected, "got", r.Status)
		}
	}

	nodeProgress := func(ip string) int {
		prog, err := s.GetProgress(ctx, &unit, runID, ip)
		if err != nil {
			t.Fatal(err)
		}

		v := 0
		for _, p := range prog {
			v += p.PercentComplete()
		}
		if l := len(prog); l > 0 {
			v /= l
		}
		return v
	}

	waitNodeProgress := func(ip string, percent int) {
		for {
			p := nodeProgress(ip)
			if p >= percent {
				break
			}

			time.Sleep(500 * time.Millisecond)
		}
	}

	assertNodeProgress := func(ip string, percent int) {
		p := nodeProgress(ip)
		if p < percent {
			t.Fatal("no progress", "expected", percent, "got", p)
		}
	}

	wait := func() {
		time.Sleep(2 * time.Second)
	}

	// Given unit
	if err := s.PutUnit(ctx, &unit); err != nil {
		t.Fatal(err)
	}

	// When run repair
	if err := s.Repair(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node0 advances
	assertNodeProgress(node0, 1)

	// When run another repair
	// Then run fails
	if err := s.Repair(ctx, &unit, uuid.NewTime()); err == nil || err.Error() != "repair already in progress" {
		t.Fatal("expected error", err)
	}

	// When node0 is 1/2 repaired
	waitNodeProgress(node0, 50)

	// And
	if err := s.StopRun(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusStopping
	assertStatus(repair.StatusStopping)

	// When wait
	wait()

	// Then status is StatusStopped
	assertStatus(repair.StatusStopped)

	// When connectivity fails
	hrt.SetInterceptor(mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		defer hrt.SetInterceptor(nil)
		return nil, errors.New("test")
	}))

	// And create a new task
	runID = uuid.NewTime()

	// Then run fails
	if err := s.Repair(ctx, &unit, runID); err == nil || !strings.Contains(err.Error(), "test") {
		t.Fatal(err)
	}

	// And status is StatusError
	assertStatus(repair.StatusError)

	// When create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node0 continues
	assertNodeProgress(node0, 50)

	// When errors occur
	hrt.SetInterceptor(failRepairInterceptor)
	time.AfterFunc(5*time.Second, func() {
		hrt.SetInterceptor(nil)
	})

	// When node0 is repaired
	waitNodeProgress(node0, 97)

	// And wait
	wait()

	// Then status is StatusError
	assertStatus(repair.StatusError)

	// When create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// And wait
	wait()

	// Then
	assertNodeProgress(node0, 100)

	// When node1 is 1/2 repaired
	waitNodeProgress(node1, 50)

	// And restart
	s.Close()
	wait()
	s, hrt = newTestService(t, session, config)
	s.FixRunStatus(ctx)

	// And create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node1 continues
	assertNodeProgress(node1, 50)

	// When node1 is repaired
	waitNodeProgress(node1, 100)
}

func TestServiceRepairStopOnErrorIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	const node0 = "172.16.1.3"

	// Given stop on error is true
	config := repair.DefaultConfig()
	config.StopOnError = true

	var (
		s, hrt    = newTestService(t, session, config)
		clusterID = uuid.MustRandom()
		runID     = uuid.NewTime()
		unit      = repair.Unit{ClusterID: clusterID, Keyspace: "test_repair"}
		ctx       = context.Background()
	)

	assertStatus := func(expected repair.Status) {
		if r, err := s.GetRun(ctx, &unit, runID); err != nil {
			t.Fatal(err)
		} else if r.Status != expected {
			t.Fatal("wrong status", r, "expected", expected, "got", r.Status)
		}
	}

	wait := func() {
		time.Sleep(2 * time.Second)
	}

	// And repair failing repair
	hrt.SetInterceptor(failRepairInterceptor)

	// And unit
	if err := s.PutUnit(ctx, &unit); err != nil {
		t.Fatal(err)
	}

	// When run repair
	if err := s.Repair(ctx, &unit, runID); err != nil {
		t.Fatal(err)
	}

	// And wait
	wait()

	// Then repair stopped
	assertStatus(repair.StatusError)

	// And errors are recorded
	prog, err := s.GetProgress(ctx, &unit, runID, node0)
	if err != nil {
		t.Fatal(err)
	}
	if len(prog) != 2 {
		t.Fatal("expected 2 shards")
	}
	for _, p := range prog {
		if p.SegmentError != config.SegmentsPerRepair {
			t.Error("expected", config.SegmentsPerRepair, "failed segments, got", p.SegmentError)
		}
		if p.SegmentSuccess != 0 {
			t.Error("expected no successful segments")
		}
		if len(p.SegmentErrorStartTokens) != 1 {
			t.Error("expected 1 error start token, got", len(p.SegmentErrorStartTokens))
		}
	}
}

func newTestService(t *testing.T, session *gocql.Session, c repair.Config) (*repair.Service, *mermaidtest.HackableRoundTripper) {
	logger := log.NewDevelopment()

	rt := mermaidtest.NewHackableRoundTripper(ssh.NewDevelopmentTransport())

	s, err := repair.NewService(
		session,
		c,
		func(_ context.Context, id uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{
				ID:   id,
				Name: "test_cluster",
			}, nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			c, err := scyllaclient.NewClient(mermaidtest.ManagedClusterHosts, rt, logger.Named("scylla"))
			if err != nil {
				return nil, err
			}
			return scyllaclient.WithConfig(c, scyllaclient.Config{
				"shard_count": float64(2),
			}), nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s, rt
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	mermaidtest.ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

var failRepairInterceptor = mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || !strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
		return nil, nil
	}

	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`"FAILED"`)),
		Request:    req,
		Header:     make(http.Header, 0),
	}, nil
})
