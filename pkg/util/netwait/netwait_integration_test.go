// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package netwait

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestWaiterTimeoutIntegration(t *testing.T) {
	host := ManagedClusterHost()

	blockCQL(t, host)
	defer unblockCQL(t, host)

	w := &Waiter{
		DialTimeout:  5 * time.Millisecond,
		RetryBackoff: 10 * time.Millisecond,
		MaxAttempts:  10,
	}
	_, err := w.WaitAnyAddr(context.Background(), net.JoinHostPort(host, "9042"))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "i/o timeout") {
		t.Errorf("expected timeout error, got %s", err)
	}
}

func blockCQL(t *testing.T, h string) {
	stdout, stderr, err := ExecOnHost(h, CmdBlockScyllaCQL)
	if err != nil {
		t.Error(errors.Wrapf(err, "block host: %s, stdout %s, stderr %s", h, stdout, stderr))
	}
}

func unblockCQL(t *testing.T, h string) {
	stdout, stderr, err := ExecOnHost(h, CmdUnblockScyllaCQL)
	if err != nil {
		t.Error(errors.Wrapf(err, "unblock host: %s, stdout %s, stderr %s", h, stdout, stderr))
	}
}
