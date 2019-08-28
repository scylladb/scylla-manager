// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/httputil/middleware"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

func TestRetryWithTimeoutIntegration(t *testing.T) {
	hosts, err := getHosts()
	if err != nil {
		t.Fatal(err)
	}

	var table = []struct {
		block   int
		timeout bool
	}{
		{
			block:   1,
			timeout: false,
		},
		{
			block:   2,
			timeout: false,
		},
		{
			block:   len(hosts) - 1,
			timeout: false,
		},
		{
			block:   len(hosts),
			timeout: true,
		},
	}

	for i, test := range table {
		t.Run(fmt.Sprintf("%d blocking %d nodes", i, test.block), func(t *testing.T) {
			if err := testRetry(hosts, test.block, test.timeout); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func getHosts() ([]string, error) {
	client, err := scyllaclient.NewClient(scyllaclient.DefaultConfigWithHosts(ManagedClusterHosts), log.NewDevelopment())
	if err != nil {
		return nil, err
	}
	return client.Hosts(context.Background())
}

func testRetry(hosts []string, n int, shouldTimeout bool) error {
	blockedHosts := make([]string, 0, len(hosts))

	block := func(ctx context.Context, hosts []string) error {
		for _, h := range hosts {
			stdout, stderr, err := ExecOnHost(h, CmdBlockScyllaREST)
			if err != nil {
				return errors.Wrapf(err, "block failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
			blockedHosts = append(blockedHosts, h)
		}
		return nil
	}

	unblock := func(ctx context.Context) error {
		for _, h := range blockedHosts {
			stdout, stderr, err := ExecOnHost(h, CmdUnblockScyllaREST)
			if err != nil {
				return errors.Wrapf(err, "unblock failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
		}
		return nil
	}

	config := scyllaclient.DefaultConfig()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n+1)*config.RequestTimeout)
	defer cancel()
	defer unblock(context.Background())

	if err := block(ctx, hosts[0:n]); err != nil {
		return err
	}

	config.Hosts = hosts
	triedHosts := make(map[string]int)
	config.Transport = hostRecorder(scyllaclient.DefaultTransport(), triedHosts)

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		return err
	}

	if _, err = client.Hosts(ctx); err != nil {
		if shouldTimeout {
			if !strings.Contains(err.Error(), "Client.Timeout") {
				return errors.Errorf("call error but no timeout, error=%v", err)
			}
		} else {
			return errors.Errorf("expected no error, got %v", err)
		}
	}

	if !shouldTimeout {
		for _, host := range blockedHosts {
			if cnt, ok := triedHosts[host]; ok && cnt > 1 {
				return errors.Errorf("tried blocked host %s %d times", host, cnt)
			}
		}
	} else {
		if len(blockedHosts) < len(triedHosts) {
			return errors.Errorf("did not try the expected number of hosts, expected %d, got %d", len(blockedHosts), len(triedHosts))
		}
	}

	return nil
}

func hostRecorder(parent http.RoundTripper, triedHosts map[string]int) http.RoundTripper {
	return middleware.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		if _, ok := triedHosts[req.Host]; !ok {
			triedHosts[req.Host] = 1
		} else {
			triedHosts[req.Host] = triedHosts[req.Host] + 1
		}
		r, e := parent.RoundTrip(req)
		return r, e
	})
}
