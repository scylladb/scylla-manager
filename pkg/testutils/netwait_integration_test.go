// Copyright (C) 2017 ScyllaDB

//go:build all || integration

package testutils

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/netwait"
)

func TestWaiterTimeoutIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}
	host := ManagedClusterHost()

	err := RunIptablesCommand(t, host, CmdBlockScyllaCQL)
	if err != nil {
		t.Fatal(err)
	}
	defer RunIptablesCommand(t, host, CmdUnblockScyllaCQL)

	w := &netwait.Waiter{
		DialTimeout:  5 * time.Millisecond,
		RetryBackoff: 10 * time.Millisecond,
		MaxAttempts:  10,
	}
	_, err = w.WaitAnyAddr(context.Background(), net.JoinHostPort(host, "9042"))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "i/o timeout") {
		t.Errorf("expected timeout error, got %s", err)
	}
}
