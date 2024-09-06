// Copyright (C) 2017 ScyllaDB

package schedules

import (
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestCronInDifferentLocations(t *testing.T) {
	cronHour := timeutc.Now().Add(-8 * time.Hour).Hour()
	cronTime := fmt.Sprintf("0 %d * * *", cronHour)

	c, err := NewCronTrigger(cronTime)
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

	// Must use a date where EST and CET locations are actually 6 hours apart.
	// A few weeks a year they are 5 or 7 due to DST.
	now := time.Date(2022, 1, 15, 12, 0o5, 0o1, 0, time.UTC)

	n0 := c.Next(now.In(l0))
	n1 := c.Next(now.In(l1))
	t.Log(n0)
	t.Log(n1)

	if n0.Location() != l0 {
		t.Fatal("Wrong location")
	}
	// Next cron can be within same day, or the next day
	if n1.Sub(n0) != 6*time.Hour && n0.Sub(n1) != 18*time.Hour {
		t.Fatal(n1.Sub(n0))
	}
}

func TestCronUTC(t *testing.T) {
	c, err := NewCronTrigger("0 4 * * *")
	if err != nil {
		t.Fatal(err)
	}

	n := c.Next(timeutc.Now())
	if n.Hour() != 4 || n.Location() != time.UTC {
		t.Fatal(n)
	}
}
