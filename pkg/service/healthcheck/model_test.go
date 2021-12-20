// Copyright (C) 2021 ScyllaDB

package healthcheck

import (
	"testing"
)

func TestModelModeMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	for _, k := range []Mode{CQLMode, RESTMode, AlternatorMode} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var v Mode
		if err := v.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != v {
			t.Errorf("Got %s, expected %s", v, k)
		}
	}
}
