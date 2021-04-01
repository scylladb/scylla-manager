// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func Dump(t *testing.T, c ...prometheus.Collector) string {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	for i := range c {
		if err := reg.Register(c[i]); err != nil {
			t.Fatalf("registering collector failed: %s", err)
		}
	}
	got, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics failed: %s", err)
	}

	var gotBuf bytes.Buffer
	enc := expfmt.NewEncoder(&gotBuf, expfmt.FmtText)
	for _, mf := range got {
		if err := enc.Encode(mf); err != nil {
			t.Fatalf("encoding gathered metrics failed: %s", err)
		}
	}

	return gotBuf.String()
}
