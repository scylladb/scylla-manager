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
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/httpx"
	"go.uber.org/zap/zapcore"
)

func TestWorkerCount(t *testing.T) {
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

			v := workerCount(ranges)
			if v != test.Count {
				t.Errorf("workerCount()=%d expected %d", v, test.Count)
			}
		})
	}
}

func TestWorkerRun(t *testing.T) {
	var (
		in           = make(chan job)
		out          = make(chan jobResult)
		logger       = log.NewDevelopmentWithLevel(zapcore.DebugLevel)
		hrt          = NewHackableRoundTripper(scyllaclient.DefaultTransport())
		c            = newTestClient(t, hrt, logger)
		ctx          = context.Background()
		pollInterval = 50 * time.Millisecond
	)
	hrt.SetInterceptor(successfulInterceptor())
	w := newWorker(in, out, c, logger, newNopProgressManager(), pollInterval)

	go func() {
		if err := w.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		for i := 0; i < 3; i++ {
			in <- job{
				Host: fmt.Sprintf("h%d", i),
				Ranges: []*tableTokenRange{
					{
						Keyspace:   "k1",
						Table:      fmt.Sprintf("t%d", i),
						Pos:        0,
						StartToken: 1,
						EndToken:   2,
						Replicas:   []string{"h0", "h1", "h2"},
					},
					{
						Keyspace:   "k1",
						Table:      fmt.Sprintf("t%d", i),
						Pos:        1,
						StartToken: 3,
						EndToken:   4,
						Replicas:   []string{"h0", "h1", "h2"},
					},
				},
			}
		}
		close(in)
	}()

	for i := 0; i < 3; i++ {
		<-out
	}
}

var commandCounter int32

func successfulInterceptor() http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
			return nil, nil
		}

		resp := &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Request:    req,
			Header:     make(http.Header, 0),
		}

		switch req.Method {
		case http.MethodGet:
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", scyllaclient.CommandSuccessful)))
		case http.MethodPost:
			id := atomic.AddInt32(&commandCounter, 1)
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprint(id)))
		}

		return resp, nil
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
