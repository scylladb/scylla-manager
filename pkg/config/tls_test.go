// Copyright (C) 2017 ScyllaDB

package config

import "testing"

func TestTLSVersionMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	for _, k := range []TLSVersion{TLSv13, TLSv12, TLSv10} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var v TLSVersion
		if err := v.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != v {
			t.Errorf("Got %s, expected %s", v, k)
		}
	}
}
