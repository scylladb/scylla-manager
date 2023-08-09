// Copyright (C) 2017 ScyllaDB

package parallel

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/atomic"
)

func TestRun(t *testing.T) {
	t.Parallel()

	const (
		n    = 50
		wait = 5 * time.Millisecond
	)

	table := []struct {
		Name     string
		Limit    int
		Duration time.Duration
	}{
		// This test is flaky under race
		//{
		//	Name:     "No limit",
		//	Duration: wait,
		//},
		{
			Name:     "One by one",
			Limit:    1,
			Duration: n * wait,
		},
		{
			Name:     "Five by five",
			Limit:    5,
			Duration: n / 5 * wait,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			active := atomic.NewInt32(0)
			f := func(i int) error {
				v := active.Inc()
				if test.Limit != NoLimit {
					if v > int32(test.Limit) {
						t.Errorf("limit exeded, got %d", v)
					}
				}
				time.Sleep(wait)
				active.Dec()
				return nil
			}

			start := timeutc.Now()
			if err := Run(n, test.Limit, f, NopNotify); err != nil {
				t.Error("Run() error", err)
			}
			d := timeutc.Since(start)
			if a, b := testutils.EpsilonRange(test.Duration); d < a || d > b {
				t.Errorf("Run() not within expected time margin %v got %v", test.Duration, d)
			}
		})
	}
}

func TestIsErrAbort(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		if ok, err := isErrAbort(Abort(nil)); !ok || err != nil {
			t.Errorf("isErrAbort() = (%v, %v), expected (%v, %v))", ok, err, true, nil)
		}
	})

	t.Run("not nil", func(t *testing.T) {
		t.Parallel()

		err := errors.New("too")

		if ok, inner := isErrAbort(Abort(err)); !ok || inner != err {
			t.Errorf("isErrAbort() = (%v, %v), expected (%v, %v))", ok, inner, true, err)
		}
	})
}

func TestAbort(t *testing.T) {
	t.Parallel()

	called := atomic.NewInt32(0)
	f := func(i int) error {
		called.Inc()
		return Abort(errors.New("boo"))
	}

	if err := Run(10, 1, f, NopNotify); err == nil {
		t.Error("Run() expected error")
	}

	if c := called.Load(); c != 1 {
		t.Errorf("Called %d times expected 1", c)
	}
}

func TestEmpty(t *testing.T) {
	t.Parallel()

	if err := Run(0, NoLimit, nil, NopNotify); err != nil {
		t.Fatal("Run() error", err)
	}
}

func TestRunMap(t *testing.T) {
	t.Parallel()
	testMap1 := map[string]string{"1": "1", "2": "2", "3": "3", "4": "4", "5": "5"}

	testExp1 := map[string]string{"1": "11", "2": "22", "3": "33", "4": "44", "5": "55"}

	result := map[string]string{}
	mut := sync.Mutex{}
	onEachElem := func(key, val string) error {
		mut.Lock()
		result[key] = key + val
		mut.Unlock()
		return nil
	}

	err := RunMap[string, string](testMap1, onEachElem, nil, 2)
	if err != nil {
		t.Fatalf("wrong work RunMap function, error:%s", err)
	}
	if !reflect.DeepEqual(testExp1, result) {
		t.Fatal("wrong work RunMap function")
	}
	result = map[string]string{}
	testMap2 := map[string]string{"1": "1"}
	testExp2 := map[string]string{"1": "11"}

	err = RunMap[string, string](testMap2, onEachElem, nil, 1)
	if err != nil {
		t.Fatalf("wrong work RunMap function, error:%s", err)
	}
	if !reflect.DeepEqual(testExp2, result) {
		t.Fatal(fmt.Sprintf("wrong work RunMap function, \nexpected:%v\nrecieved:%v\n", testExp2, result))
	}
}
