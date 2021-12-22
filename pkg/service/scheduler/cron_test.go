// Copyright (C) 2021 ScyllaDB

package scheduler

import (
	"testing"
	"time"
)

func TestCronMarshalUnmarshal(t *testing.T) {
	spec := "@every 15s"

	var cron Cron
	if err := cron.UnmarshalText([]byte(spec)); err != nil {
		t.Fatal(err)
	}
	b, _ := cron.MarshalText()
	if string(b) != spec {
		t.Fatalf("MarshalText() = %s, expected %s", string(b), spec)
	}
}

func TestNewCronEvery(t *testing.T) {
	c := NewCronEvery(15 * time.Second)
	if c.IsZero() {
		t.Fatal()
	}
}

func TestEmptyCron(t *testing.T) {
	var cron Cron
	if err := cron.UnmarshalText(nil); err != nil {
		t.Fatal(err)
	}
	cron.Next(now())
}
