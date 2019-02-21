// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/httputil"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

func TestClientDialOnceAndCloseIntegration(t *testing.T) {
	// Protects conns.
	var mu sync.Mutex
	var conns []net.Conn

	s := NewSSHTransport()
	d := s.DialContext
	s.DialContext = func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
		conn, err = d(ctx, network, addr)
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		return
	}

	client, err := scyllaclient.NewClient(ManagedClusterHosts, s, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := client.Keyspaces(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 1 {
		t.Fatal("expected dial once, got", len(conns))
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conns[0].SetDeadline(time.Time{}); err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestClientClosestDCIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(ManagedClusterHosts, NewSSHTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	dcs := map[string][]string{
		"dc1": ManagedClusterHosts,
		"xx":  {"xx.xx.xx.xx"},
	}

	closest, err := client.ClosestDC(context.Background(), dcs)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(closest, []string{"dc1", "xx"}); diff != "" {
		t.Fatal(closest, diff)
	}
}

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
	client, err := scyllaclient.NewClient(ManagedClusterHosts, NewSSHTransport(), log.NewDevelopment())
	if err != nil {
		return nil, err
	}
	return client.Hosts(context.Background())
}

func testRetry(hosts []string, n int, shouldTimeout bool) error {
	blockedHosts := make([]string, 0, len(hosts))

	block := func(ctx context.Context, hosts []string) error {
		for _, h := range hosts {
			stdout, stderr, err := ExecOnHost(context.Background(), h, CmdBlockScyllaAPI)
			if err != nil {
				return errors.Wrapf(err, "block failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
			blockedHosts = append(blockedHosts, h)
		}
		return nil
	}

	unblock := func(ctx context.Context) error {
		for _, h := range blockedHosts {
			stdout, stderr, err := ExecOnHost(ctx, h, CmdUnblockScyllaAPI)
			if err != nil {
				return errors.Wrapf(err, "unblock failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
			}
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n+1)*scyllaclient.RequestTimeout)
	defer cancel()
	defer unblock(context.Background())

	if err := block(ctx, hosts[0:n]); err != nil {
		return err
	}

	triedHosts := make(map[string]int)

	client, err := scyllaclient.NewClient(hosts, hostRecorder(NewSSHTransport(), triedHosts), log.NewDevelopment())
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
	return httputil.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		if _, ok := triedHosts[req.Host]; !ok {
			triedHosts[req.Host] = 1
		} else {
			triedHosts[req.Host] = triedHosts[req.Host] + 1
		}
		r, e := parent.RoundTrip(req)
		return r, e
	})
}
