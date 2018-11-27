// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/httputil"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
)

func TestClientSSHTransportIntegration(t *testing.T) {
	client, err := NewClient(mermaidtest.ManagedClusterHosts, ssh.NewDevelopmentTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := client.Keyspaces(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientClosestDCIntegration(t *testing.T) {
	client, err := NewClient(mermaidtest.ManagedClusterHosts, ssh.NewDevelopmentTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	dcs := map[string][]string{
		"dc1": mermaidtest.ManagedClusterHosts,
		"xx":  {"xx.xx.xx.xx"},
	}

	dc, err := client.ClosestDC(context.Background(), dcs)
	if err != nil {
		t.Fatal(err)
	}
	if dc != "dc1" {
		t.Fatalf("expected %s, got %s", "dc1", dc)
	}
}

func TestBlockedHosts(t *testing.T) {
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
			block:   6,
			timeout: true,
		},
	}

	hosts, err := getHosts()
	if err != nil {
		t.Fatal(err)
	}

	hb := mermaidtest.NewScyllaExecutor()
	defer hb.Close()

	for _, tab := range table {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		blockedHosts, err := block(ctx, hb, hosts[0:tab.block]...)
		if err != nil {
			t.Fatalf("unable to block Scylla API, error=%s, successfully blocked hosts %s", err, blockedHosts)
		}
		cancel()

		cleanup := func(ctx context.Context, cancel context.CancelFunc) {
			defer cancel()
			if host, stdout, stderr, err := unblock(ctx, hb, blockedHosts...); err != nil {
				t.Fatalf("unable to unblock Scylla API, error=%s, host=%s, stdout=%s, stderr=%s", err, host, stdout, stderr)
			}
		}

		triedHosts := make(map[string]int)
		transport := ssh.NewDevelopmentTransport()
		client, err := NewClient(hosts, hostRecorder(transport, triedHosts), log.NewDevelopment())
		if err != nil {
			cleanup(context.WithTimeout(context.Background(), 5*time.Second))
			t.Fatal(err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), 40*time.Second)

		var errMsg string
		_, err = client.Hosts(ctx)
		if err != nil {
			if tab.timeout {
				if !strings.Contains(err.Error(), "Client.Timeout") {
					errMsg = fmt.Sprintf("call error but no timeout, error=%T\n%v\n%s", err, err, err)
				}
			} else {
				errMsg = fmt.Sprintf("expected no error, error=%s", err)
			}
		}

		cleanup(context.WithTimeout(context.Background(), 5*time.Second))
		cancel()

		if errMsg != "" {
			t.Fatal(errMsg)
		}

		if !tab.timeout {
			for _, host := range blockedHosts {
				if cnt, ok := triedHosts[host]; ok && cnt > 1 {
					t.Fatalf("tried blocked host %s %d times", host, cnt)
				}
			}
		} else {
			if len(blockedHosts) < len(triedHosts) {
				t.Fatalf("did not try the expected number of hosts, expected %d, got %d", len(blockedHosts), len(triedHosts))
			}
		}
	}
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

func getHosts() ([]string, error) {
	transport := ssh.NewDevelopmentTransport()
	client, err := NewClient(mermaidtest.ManagedClusterHosts, transport, log.NewDevelopment())
	if err != nil {
		return nil, err
	}
	return client.Hosts(context.Background())
}

func block(ctx context.Context, se *mermaidtest.ScyllaExecutor, hosts ...string) ([]string, error) {
	blockedHosts := make([]string, 0, len(hosts))
	for _, host := range hosts {
		_, _, err := se.ExecOnHost(ctx, host, mermaidtest.CmdBlockScyllaAPI)
		if err != nil {
			return blockedHosts, err
		}
		blockedHosts = append(blockedHosts, host)
	}
	return blockedHosts, nil
}

func unblock(ctx context.Context, se *mermaidtest.ScyllaExecutor, hosts ...string) (string, string, string, error) {
	for _, host := range hosts {
		stdout, stderr, err := se.ExecOnHost(ctx, host, mermaidtest.CmdUnblockScyllaAPI)
		if err != nil {
			return host, stdout, stderr, err
		}
	}
	return "", "", "", nil
}
