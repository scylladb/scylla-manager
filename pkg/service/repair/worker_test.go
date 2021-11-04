// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/dht"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestWorkerRun(t *testing.T) {
	var (
		logger                    = log.NewDevelopmentWithLevel(zapcore.DebugLevel)
		hrt                       = NewHackableRoundTripper(scyllaclient.DefaultTransport())
		c                         = newTestClient(t, hrt, logger)
		ctx                       = context.Background()
		pollInterval              = 50 * time.Millisecond
		longPollingTimeoutSeconds = 2
		partitioner               = dht.NewMurmur3Partitioner(2, 12)
		hostPartitioner           = map[string]*dht.Murmur3Partitioner{"h1": partitioner, "h2": partitioner}
		run                       = &Run{ID: uuid.NewTime(), TaskID: uuid.NewTime()}
	)

	t.Run("successful run", func(t *testing.T) {
		in := make(chan job)
		out := make(chan jobResult)
		ranges := []tokenRange{
			{StartToken: 0, EndToken: 5}, {StartToken: 6, EndToken: 10},
		}
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2))

		w := newWorker(run, in, out, c, newNopProgressManager(), hostPartitioner, scyllaFeatures(true, true), TypeRowLevel, pollInterval, longPollingTimeoutSeconds, logger)

		go func() {
			if err := w.Run(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		go func() {
			for i := 1; i <= 2; i++ {
				in <- job{
					Host: fmt.Sprintf("h%d", i),
					Ranges: []*tableTokenRange{
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        0,
							StartToken: 0,
							EndToken:   5,
							Replicas:   []string{"h1", "h2"},
						},
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        1,
							StartToken: 6,
							EndToken:   10,
							Replicas:   []string{"h1", "h2"},
						},
					},
				}
			}
			close(in)
		}()

		for i := 0; i < 2; i++ {
			res := <-out
			if res.Err != nil {
				t.Error(res.Err)
			}
			if res.Ranges[0].StartToken != ranges[0].StartToken || res.Ranges[1].EndToken != ranges[1].EndToken {
				t.Errorf("Unexpected ranges %+v", res.Ranges)
			}
		}
	})

	t.Run("fail run", func(t *testing.T) {
		in := make(chan job)
		out := make(chan jobResult)
		ranges := []tokenRange{
			{StartToken: 0, EndToken: 5}, {StartToken: 6, EndToken: 10},
		}
		hrt.SetInterceptor(repairInterceptor(false, ranges, 2))

		w := newWorker(run, in, out, c, newNopProgressManager(), hostPartitioner, scyllaFeatures(true, false), TypeRowLevel, pollInterval, longPollingTimeoutSeconds, logger)

		go func() {
			if err := w.Run(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		go func() {
			for i := 1; i <= 2; i++ {
				in <- job{
					Host: fmt.Sprintf("h%d", i),
					Ranges: []*tableTokenRange{
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        0,
							StartToken: 0,
							EndToken:   5,
							Replicas:   []string{"h1", "h2"},
						},
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        1,
							StartToken: 6,
							EndToken:   10,
							Replicas:   []string{"h1", "h2"},
						},
					},
				}
			}
			close(in)
		}()

		for i := 0; i < 2; i++ {
			res := <-out
			if res.Err == nil {
				t.Error("Expected error")
			}
			if res.Ranges[0].StartToken != ranges[0].StartToken || res.Ranges[1].EndToken != ranges[1].EndToken {
				t.Errorf("Unexpected ranges %+v", res.Ranges)
			}
		}
	})

	t.Run("legacy run", func(t *testing.T) {
		in := make(chan job)
		out := make(chan jobResult)
		ranges := []tokenRange{
			{StartToken: 3689195723611658698, EndToken: 3689195723611658798}, {StartToken: -8022912513662303546, EndToken: -8022912513662303446},
		}
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2))

		w := newWorker(run, in, out, c, newNopProgressManager(), hostPartitioner, scyllaFeatures(false, false), TypeLegacy, pollInterval, longPollingTimeoutSeconds, logger)

		go func() {
			if err := w.Run(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		go func() {
			for i := 1; i <= 2; i++ {
				in <- job{
					Host: fmt.Sprintf("h%d", i),
					Ranges: []*tableTokenRange{
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        0,
							StartToken: ranges[0].StartToken,
							EndToken:   ranges[0].EndToken,
							Replicas:   []string{"h1", "h2"},
						},
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        1,
							StartToken: ranges[1].StartToken,
							EndToken:   ranges[1].EndToken,
							Replicas:   []string{"h1", "h2"},
						},
					},
				}
			}
			close(in)
		}()

		for i := 0; i < 2; i++ {
			res := <-out
			if res.Err != nil {
				t.Error(res.Err)
			}
			if res.Ranges[0].StartToken != ranges[0].StartToken || res.Ranges[1].EndToken != ranges[1].EndToken {
				t.Errorf("Unexpected ranges %+v", res.Ranges)
			}
		}
	})

	t.Run("force row level repair", func(t *testing.T) {
		in := make(chan job)
		out := make(chan jobResult)
		ranges := []tokenRange{
			{StartToken: 0, EndToken: 5}, {StartToken: 6, EndToken: 10},
		}
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2))

		w := newWorker(run, in, out, c, newNopProgressManager(), hostPartitioner, scyllaFeatures(true, true), TypeRowLevel, pollInterval, longPollingTimeoutSeconds, logger)

		go func() {
			if err := w.Run(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		go func() {
			for i := 0; i < 2; i++ {
				in <- job{
					Host: fmt.Sprintf("h%d", i),
					Ranges: []*tableTokenRange{
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        0,
							StartToken: 0,
							EndToken:   5,
							Replicas:   []string{"h1", "h2"},
						},
						{
							Keyspace:   "k1",
							Table:      fmt.Sprintf("t%d", i),
							Pos:        1,
							StartToken: 6,
							EndToken:   10,
							Replicas:   []string{"h1", "h2"},
						},
					},
				}
			}
			close(in)
		}()

		for i := 0; i < 2; i++ {
			res := <-out
			if res.Err != nil {
				t.Error(res.Err)
			}
			if res.Ranges[0].StartToken != ranges[0].StartToken || res.Ranges[1].EndToken != ranges[1].EndToken {
				t.Errorf("Unexpected ranges %+v", res.Ranges)
			}
		}
	})
}

func scyllaFeatures(rowLevel, longPolling bool) map[string]scyllaclient.ScyllaFeatures {
	out := make(map[string]scyllaclient.ScyllaFeatures)

	for i := 0; i < 5; i++ {
		out[fmt.Sprintf("h%d", i)] = scyllaclient.ScyllaFeatures{
			rowLevel, longPolling,
		}
	}

	return out
}

var commandCounter int32

type tokenRange struct {
	StartToken int64
	EndToken   int64
}

func repairInterceptor(success bool, ranges []tokenRange, shardCount int) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp := &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Request:    req,
			Header:     make(http.Header, 0),
		}
		if strings.HasPrefix(req.URL.Path, "/storage_service/describe_ring") {
			var s []map[string]interface{}
			for i := range ranges {
				s = append(s, map[string]interface{}{
					"start_token": fmt.Sprintf("%d", ranges[i].StartToken),
					"end_token":   fmt.Sprintf("%d", ranges[i].EndToken),
					"endpoints": []string{
						"h1",
						"h2",
					},
					"rpc_endpoints": []string{
						"h1",
						"h2",
					},
					"endpoint_details": []map[string]string{
						{
							"host":       "h1",
							"datacenter": "dc1",
							"rack":       "r1",
						},
						{
							"host":       "h2",
							"datacenter": "dc1",
							"rack":       "r1",
						},
					},
				})
			}
			res, err := json.Marshal(s)
			if err != nil {
				return nil, err
			}
			resp.Body = ioutil.NopCloser(bytes.NewBuffer(res))

			return resp, nil
		}

		if strings.HasPrefix(req.URL.Path, "/metrics") {
			s := ""
			for i := 0; i < shardCount; i++ {
				s += fmt.Sprintf("scylla_database_total_writes{shard=\"%d\",type=\"derive\"} 162\n", i)
			}
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(s))

			return resp, nil
		}

		if strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
			cmd := scyllaclient.CommandFailed
			if success {
				cmd = scyllaclient.CommandSuccessful
			}
			switch req.Method {
			case http.MethodGet:
				resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", cmd)))
			case http.MethodPost:
				id := atomic.AddInt32(&commandCounter, 1)
				resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprint(id)))
			}

			return resp, nil
		}

		if strings.HasPrefix(req.URL.Path, "/storage_service/repair_status") {
			cmd := scyllaclient.CommandFailed
			if success {
				cmd = scyllaclient.CommandSuccessful
			}
			switch req.Method {
			case http.MethodGet:
				resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", cmd)))
			}

			return resp, nil
		}

		return nil, nil
	})
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
