// Copyright (C) 2017 ScyllaDB

package trigger

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func TestCronInDifferentLocations(t *testing.T) {
	c, err := NewCron("0 4 * * *")
	if err != nil {
		t.Fatal(err)
	}
	l0, err := time.LoadLocation("CET")
	if err != nil {
		t.Fatal(err)
	}
	l1, err := time.LoadLocation("EST")
	if err != nil {
		t.Fatal(err)
	}

	now := timeutc.Now()

	n0 := c.Next(now.In(l0))
	n1 := c.Next(now.In(l1))
	t.Log(n0)
	t.Log(n1)

	if n0.Location() != l0 {
		t.Fatal("Wrong location")
	}
	if n1.Sub(n0) != 6*time.Hour {
		t.Fatal(n1.Sub(n0))
	}
}

func TestCronUTC(t *testing.T) {
	c, err := NewCron("0 4 * * *")
	if err != nil {
		t.Fatal(err)
	}

	n := c.Next(timeutc.Now())
	if n.Hour() != 4 || n.Location() != time.UTC {
		t.Fatal(n)
	}
}
