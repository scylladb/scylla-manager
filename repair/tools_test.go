package repair

import (
	"testing"
)

func TestClientTopologyHash(t *testing.T) {
	t.Parallel()

	v, err := topologyHash([]int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "17cb299f-0000-4000-9599-a4a200000000" {
		t.Fatal(v)
	}
}
