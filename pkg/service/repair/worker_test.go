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

func TestMaxParallelRepairs(t *testing.T) {
	table := []struct {
		Name      string
		InputFile string
		Count     int
	}{
		{
			Name:      "RF=2",
			InputFile: "testdata/worker_count/simple_rf2.json",
			Count:     3,
		},
		{
			Name:      "RF=3",
			InputFile: "testdata/worker_count/simple_rf3.json",
			Count:     2,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			var content []struct {
				Endpoints []string `json:"endpoints"`
			}
			data, err := ioutil.ReadFile(test.InputFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &content); err != nil {
				t.Fatal(err)
			}
			var ranges []scyllaclient.TokenRange
			for i := range content {
				ranges = append(ranges, scyllaclient.TokenRange{Replicas: content[i].Endpoints})
			}

			if v := maxParallelRepairs(ranges); v != test.Count {
				t.Errorf("maxParallelRepairs()=%d expected %d", v, test.Count)
			}
		})
	}
}

func TestWorkerRun(t *testing.T) {
	var (
		logger               = log.NewDevelopmentWithLevel(zapcore.DebugLevel)
		hrt                  = NewHackableRoundTripper(scyllaclient.DefaultTransport())
		c                    = newTestClient(t, hrt, logger)
		ctx                  = context.Background()
		pollInterval         = 50 * time.Millisecond
		partitioner          = dht.NewMurmur3Partitioner(2, 12)
		hostPartitioner      = map[string]*dht.Murmur3Partitioner{"h1": partitioner, "h2": partitioner}
		emptyHostPartitioner = make(map[string]*dht.Murmur3Partitioner)
		run                  = &Run{ID: uuid.NewTime(), TaskID: uuid.NewTime(), clusterName: "test-cluster"}
	)

	t.Run("successful run", func(t *testing.T) {
		in := make(chan job)
		out := make(chan jobResult)
		ranges := []tokenRange{
			{StartToken: 0, EndToken: 5}, {StartToken: 6, EndToken: 10},
		}
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2, "3.1.0-0.20191012.9c3cdded9"))

		w := newWorker(run, in, out, c, newNopProgressManager(), emptyHostPartitioner, pollInterval, logger)

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
		hrt.SetInterceptor(repairInterceptor(false, ranges, 2, "3.1.0-0.20191012.9c3cdded9"))

		w := newWorker(run, in, out, c, newNopProgressManager(), emptyHostPartitioner, pollInterval, logger)

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
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2, "3.0.0-0.20191012.9c3cdded9"))

		w := newWorker(run, in, out, c, newNopProgressManager(), hostPartitioner, pollInterval, logger)

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
		hrt.SetInterceptor(repairInterceptor(true, ranges, 2, "3.0.0-0.20191012.9c3cdded9"))

		w := newWorker(run, in, out, c, newNopProgressManager(), emptyHostPartitioner, pollInterval, logger)

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

var commandCounter int32

type tokenRange struct {
	StartToken int64
	EndToken   int64
}

func repairInterceptor(success bool, ranges []tokenRange, shardCount int, version string) http.RoundTripper {
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

		if strings.HasPrefix(req.URL.Path, "/storage_service/scylla_release_version") {
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(`"` + version + `"`))

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
