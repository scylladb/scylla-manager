// Copyright (C) 2017 ScyllaDB

//go:build all || integration
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
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
)

func TestRetryWithTimeoutIntegration(t *testing.T) {
	hosts, err := allHosts()
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

	for i := range table {
		test := table[i]

		t.Run(fmt.Sprintf("block %d nodes", test.block), func(t *testing.T) {
			if err := testRetry(hosts, test.block, test.timeout); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func allHosts() ([]string, error) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
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
				return errors.Wrapf(err, "block host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
			blockedHosts = append(blockedHosts, h)
		}
		return nil
	}

	unblock := func(ctx context.Context) error {
		for _, h := range blockedHosts {
			stdout, stderr, err := ExecOnHost(h, CmdUnblockScyllaREST)
			if err != nil {
				return errors.Wrapf(err, "unblock host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
		}
		return nil
	}

	triedHosts := make(map[string]int)

	config := scyllaclient.TestConfig(hosts, AgentAuthToken())
	config.Transport = hostRecorder(scyllaclient.DefaultTransport(), triedHosts)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n+1)*config.Timeout)
	defer cancel()
	defer unblock(context.Background())

	if err := block(ctx, hosts[0:n]); err != nil {
		return err
	}

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		return err
	}

	if _, err = client.Hosts(ctx); err != nil {
		if shouldTimeout {
			if !strings.HasSuffix(err.Error(), fmt.Sprintf("after %s: context deadline exceeded", client.Config().Timeout)) {
				return errors.Errorf("call error %s, expected timeout", err)
			}
		} else {
			return errors.Errorf("call error %s, expected no errors", err)
		}
	}

	if !shouldTimeout {
		for _, host := range blockedHosts {
			if triedHosts[host] > 1 {
				return errors.Errorf("tried blocked host %s %d times", host, triedHosts[host])
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
	return httpx.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		if _, ok := triedHosts[req.Host]; !ok {
			triedHosts[req.Host] = 1
		} else {
			triedHosts[req.Host] = triedHosts[req.Host] + 1
		}
		r, e := parent.RoundTrip(req)
		return r, e
	})
}
